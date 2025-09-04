import asyncio
import os
import shutil
import ffmpeg
from video_artifact_processing_engine.aws.aws_client import S3Service, get_public_url
from video_artifact_processing_engine.config import config
from video_artifact_processing_engine.aws.db_operations import upload_with_retry
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

logger = setup_custom_logger(__name__)

class VideoHLSConverter:
    def __init__(self, s3_service: S3Service):
        self.s3_service = s3_service

    async def transcode_to_hls(self, source_clip_path: str, output_dir: str):
        """
        Transcodes a source video to multiple HLS renditions, then deterministically
        generates and verifies a master playlist to ensure reliability.
        """
        renditions = [
            {'resolution': '1280x720', 'bitrate': '1200k', 'name': '720p'},
            {'resolution': '854x480',  'bitrate': '700k',  'name': '480p'},
            {'resolution': '640x360',  'bitrate': '400k',  'name': '360p'},
        ]
        
        logger.info(f"Starting unified HLS transcoding for {source_clip_path}")

        # --- Sections 1 and 2 remain the same ---
        input_stream = ffmpeg.input(source_clip_path)
        output_streams = []
        stream_map_string = ""
        
        hls_params = {
            'f': 'hls',
            'hls_flags': 'single_file',
            'hls_time': 6,
            'hls_playlist_type': 'vod',
            'hls_segment_type': 'fmp4',
        }

        # Build outputs with deterministic playlist and segment file names per rendition.
        # We'll also create the directories upfront.
        for i, r in enumerate(renditions):
            rendition_dir = os.path.join(output_dir, r['name'])
            os.makedirs(rendition_dir, exist_ok=True)
            output_playlist = os.path.join(rendition_dir, f"{r['name']}.m3u8")
            segment_filename = os.path.join(rendition_dir, f"{r['name']}.m4s")
            scaled_v_stream = input_stream.video.filter('scale', r['resolution'])
            
            output = ffmpeg.output(
                scaled_v_stream, input_stream.audio, output_playlist,
                vcodec='libx264',
                video_bitrate=r['bitrate'],
                preset='veryfast',
                acodec='aac',
                audio_bitrate='96k',
                **hls_params,
                **{'x264-params': 'keyint=48:min-keyint=48:scenecut=0'},
                hls_segment_filename=segment_filename
            )
            output_streams.append(output)
            stream_map_string += f"v:{i},a:{i} "

        # We'll build the command list and let ffmpeg generate rendition playlists.
        master_playlist_path = os.path.join(output_dir, 'master.m3u8')
        
        # Get the arguments for the main graph from the library
        merged_graph = ffmpeg.merge_outputs(*output_streams)
        args = merged_graph.get_args()
        
        # Manually construct the full command
        # Note: we won't rely on ffmpeg to write the master playlist; we'll create it ourselves
        # after verifying rendition playlists exist.
        cmd = ['ffmpeg', '-y'] + args

        try:
            logger.info(f"Running unified ffmpeg command: {' '.join(cmd)}")
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_message = stderr.decode() if stderr else "No stderr output."
                logger.error(f"Unified FFmpeg HLS transcoding failed with return code {process.returncode}")
                logger.error(f"FFmpeg stderr: {error_message}")
                raise RuntimeError(f"HLS Transcoding Failed: {error_message}")
                
            logger.info("Unified FFmpeg HLS transcoding completed successfully.")

        except ffmpeg.Error as e:
            logger.error(f"FFmpeg error during unified HLS command: {e.stderr.decode()}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error running unified HLS command: {e}")
            raise

        # Verification and Master playlist generation for 100% reliability
        # 1) Ensure rendition playlists and segments exist and look correct
        valid_renditions: list[tuple[str, str, str]] = []  # (name, playlist_rel, resolution)
        for r in renditions:
            rendition_dir = os.path.join(output_dir, r['name'])
            playlist_path = os.path.join(rendition_dir, f"{r['name']}.m3u8")
            segment_path = os.path.join(rendition_dir, f"{r['name']}.m4s")
            if not os.path.exists(playlist_path):
                raise RuntimeError(f"Missing rendition playlist: {playlist_path}")
            # Basic sanity check on playlist content
            try:
                with open(playlist_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                if '#EXTM3U' not in content or '#EXT-X-TARGETDURATION' not in content:
                    raise RuntimeError(f"Invalid HLS playlist content for {playlist_path}")
            except Exception as e:
                raise RuntimeError(f"Failed to read/validate rendition playlist {playlist_path}: {e}")
            # Verify we have a segment (single_file fMP4)
            if not os.path.exists(segment_path):
                # Try to detect any .m4s in the folder if name differs
                alt_segments = [f for f in os.listdir(rendition_dir) if f.endswith('.m4s')]
                if not alt_segments:
                    raise RuntimeError(f"Missing fMP4 segment for rendition: {rendition_dir}")
            valid_renditions.append((r['name'], f"{r['name']}/{r['name']}.m3u8", r['resolution']))

        # 2) Generate master playlist deterministically
        def bitrate_to_int(bitrate: str) -> int:
            # e.g. '1200k' -> 1200000
            try:
                if bitrate.endswith('k'):
                    return int(float(bitrate[:-1]) * 1000)
                if bitrate.endswith('M'):
                    return int(float(bitrate[:-1]) * 1_000_000)
                return int(bitrate)
            except Exception:
                return 800000

        master_lines = [
            '#EXTM3U',
            '#EXT-X-VERSION:7',
        ]
        for r in renditions:
            bw = bitrate_to_int(r['bitrate'])
            res = r['resolution']
            playlist_rel = f"{r['name']}/{r['name']}.m3u8"
            master_lines.append(f"#EXT-X-STREAM-INF:BANDWIDTH={bw},RESOLUTION={res},CODECS=\"avc1.4d401f,mp4a.40.2\"")
            master_lines.append(playlist_rel)

        os.makedirs(output_dir, exist_ok=True)
        with open(master_playlist_path, 'w', encoding='utf-8') as mf:
            mf.write('\n'.join(master_lines) + '\n')

        # 3) Verify master playlist exists and is valid
        if not os.path.exists(master_playlist_path):
            raise RuntimeError("Failed to create master playlist.")
        try:
            with open(master_playlist_path, 'r', encoding='utf-8') as f:
                master_content = f.read()
            if '#EXTM3U' not in master_content or '#EXT-X-STREAM-INF' not in master_content:
                raise RuntimeError("Master playlist content invalid.")
        except Exception as e:
            raise RuntimeError(f"Failed to read/validate master playlist: {e}")

        return master_playlist_path

    async def upload_hls_to_s3(self, hls_output_dir: str, s3_key_prefix: str, bucket_name: str):
        """
        Uploads HLS files and returns the master playlist URL and all uploaded keys.
        """
        # Bound upload concurrency to avoid overwhelming the network/CPU
        max_uploads = getattr(
            config,
            'max_concurrent_uploads',
            max(2, min(16, getattr(config, 'max_concurrent_processing', 3) * 2)),
        )
        sem = asyncio.Semaphore(max_uploads)

        async def guarded_upload(file_path: str, key: str):
            async with sem:
                await upload_with_retry(self.s3_service, file_path, bucket_name, key)

        upload_tasks = []
        all_s3_keys = []
        for root, _, files in os.walk(hls_output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, hls_output_dir)
                s3_key = f"{s3_key_prefix}/{relative_path.replace(os.path.sep, '/')}"
                all_s3_keys.append(s3_key)
                upload_tasks.append(guarded_upload(file_path, s3_key))
                logger.info(f"Preparing to upload {file_path} to s3://{bucket_name}/{s3_key}")
        await asyncio.gather(*upload_tasks)
        master_playlist_s3_key = f"{s3_key_prefix}/master.m3u8"
        master_playlist_url = get_public_url(bucket_name, master_playlist_s3_key)

        # Verify master playlist exists in S3 (HEAD with retries)
        for attempt in range(1, 4):
            try:
                await asyncio.to_thread(
                    self.s3_service.client.head_object,
                    Bucket=bucket_name,
                    Key=master_playlist_s3_key,
                )
                logger.info(
                    f"Verified presence of master playlist in S3 on attempt {attempt}: s3://{bucket_name}/{master_playlist_s3_key}"
                )
                break
            except Exception as e:
                logger.error(f"HEAD failed for master playlist (attempt {attempt}): {e}")
                if attempt == 3:
                    raise RuntimeError(
                        f"Master playlist not found in S3 after upload: s3://{bucket_name}/{master_playlist_s3_key}"
                    )
                await asyncio.sleep(2 ** attempt)

        # Verify all rendition playlists and segments exist (best-effort with retries)
        verify_keys = [k for k in all_s3_keys if k.endswith(('.m3u8', '.m4s'))]
        for key in verify_keys:
            for attempt in range(1, 3):
                try:
                    await asyncio.to_thread(
                        self.s3_service.client.head_object,
                        Bucket=bucket_name,
                        Key=key,
                    )
                    break
                except Exception as e:
                    if attempt == 2:
                        raise RuntimeError(f"Missing uploaded HLS artifact: s3://{bucket_name}/{key}")
                    await asyncio.sleep(1)

        return master_playlist_url, all_s3_keys