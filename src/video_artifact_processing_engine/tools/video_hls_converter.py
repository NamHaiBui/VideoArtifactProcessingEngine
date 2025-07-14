import asyncio
import os
import shutil
import ffmpeg
from video_artifact_processing_engine.aws.aws_client import S3Service
from video_artifact_processing_engine.config import config
from video_artifact_processing_engine.aws.db_operations import upload_with_retry
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

logger = setup_custom_logger(__name__)

class VideoHLSConverter:
    def __init__(self, s3_service: S3Service):
        self.s3_service = s3_service

    async def transcode_to_hls(self, source_clip_path: str, output_dir: str):
        renditions = [
            {'resolution': '1280x720', 'bitrate': '1200k', 'name': '720p'},
            {'resolution': '854x480', 'bitrate': '700k', 'name': '480p'},
            {'resolution': '640x360', 'bitrate': '400k', 'name': '360p'},
        ]
        logger.info(f"Transcoding to HLS renditions: {[r['name'] for r in renditions]}")

        transcode_tasks = []
        for r in renditions:
            rendition_dir = os.path.join(output_dir, r['name'])
            os.makedirs(rendition_dir, exist_ok=True)
            output_playlist = os.path.join(rendition_dir, f"{r['name']}.m3u8")
            
            process = (
                ffmpeg
                .input(source_clip_path)
                .output(
                    output_playlist,
                    vf=f"scale={r['resolution']}",
                    c_v='libx264', x264_params='keyint=48:min-keyint=48:scenecut=0',
                    b_v=r['bitrate'],
                    c_a='aac', b_a='96k',
                    f='hls', hls_time=6, hls_playlist_type='vod',
                    hls_segment_type='fmp4', hls_segment_filename=os.path.join(rendition_dir, 'data%02d.m4s')
                )
                .overwrite_output()
                .run_async(pipe_stdout=True, pipe_stderr=True, cmd=config.get_ffmpeg_path())
            )
            transcode_tasks.append(process.wait())
        
        await asyncio.gather(*transcode_tasks)
        
        master_playlist_content = '#EXTM3U\n#EXT-X-VERSION:3\n'
        for r in renditions:
            bandwidth = int(r['bitrate'].replace('k', '')) * 1000
            master_playlist_content += f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION={r['resolution']}\n"
            master_playlist_content += f"{r['name']}/{r['name']}.m3u8\n"
        
        master_playlist_path = os.path.join(output_dir, 'master.m3u8')
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist_content)
        logger.info("Master HLS playlist created.")
        return master_playlist_path

    async def upload_hls_to_s3(self, hls_output_dir: str, s3_key_prefix: str, bucket_name: str):
        """
        Uploads HLS files and returns the master playlist URL and all uploaded keys.
        """
        upload_tasks = []
        all_s3_keys = []
        for root, _, files in os.walk(hls_output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, hls_output_dir)
                s3_key = f"{s3_key_prefix}/{relative_path.replace(os.path.sep, '/')}"
                all_s3_keys.append(s3_key)
                upload_tasks.append(upload_with_retry(self.s3_service, file_path, bucket_name, s3_key))
        
        await asyncio.gather(*upload_tasks)
        
        master_playlist_s3_key = f"{s3_key_prefix}/master.m3u8"
        master_playlist_url = self.s3_service.get_public_url(bucket_name, master_playlist_s3_key)
        
        return master_playlist_url, all_s3_keys