
from typing import Any, Dict, List

def dynamodb_attribute_to_python_type(attr: Any) -> Any:
    """
    Convert a DynamoDB attribute value to a Python type.
    
    Args:
        attr: DynamoDB attribute value (can be dict with type keys like {'S': 'value'})
        
    Returns:
        Python value
    """
    if isinstance(attr, dict):
        # Handle DynamoDB type descriptors
        if 'S' in attr:  # String
            return attr['S']
        elif 'N' in attr:  # Number
            try:
                # Try to convert to int first, then float
                if '.' in attr['N']:
                    return float(attr['N'])
                else:
                    return int(attr['N'])
            except ValueError:
                return attr['N']  # Return as string if conversion fails
        elif 'B' in attr:  # Binary
            return attr['B']
        elif 'SS' in attr:  # String Set
            return attr['SS']
        elif 'NS' in attr:  # Number Set
            return [float(n) if '.' in n else int(n) for n in attr['NS']]
        elif 'BS' in attr:  # Binary Set
            return attr['BS']
        elif 'M' in attr:  # Map
            return {k: dynamodb_attribute_to_python_type(v) for k, v in attr['M'].items()}
        elif 'L' in attr:  # List
            return [dynamodb_attribute_to_python_type(item) for item in attr['L']]
        elif 'NULL' in attr:  # Null
            return None
        elif 'BOOL' in attr:  # Boolean
            return attr['BOOL']
        else:
            # If no type descriptor found, assume it's already a Python dict
            return attr
    else:
        return attr


def flatten_timestamps(timestamp_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Flatten DynamoDB timestamp data to simple dict.
    
    Args:
        timestamp_data: DynamoDB timestamp structure
        
    Returns:
        Dict with 'start' and 'end' keys
    """
    flattened = {}
    # logging.info(f"Flattening timestamps: {timestamp_data}")
    if isinstance(timestamp_data, dict):
        for key, value in timestamp_data.items():
            flattened[key] = dynamodb_attribute_to_python_type(value)
    # logging.info(f"Flattened timestamps: {flattened}")
    return flattened


def flatten_word_timestamps(word_timestamps_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten DynamoDB word timestamp data to simple list of dicts.
    
    Args:
        word_timestamps_data: List of DynamoDB word timestamp structures
        
    Returns:
        List of flattened word timestamp dicts
    """
    flattened_list = []
    
    for item in word_timestamps_data:
        if isinstance(item, dict):
            flattened_item = dynamodb_attribute_to_python_type(item)
            flattened_list.append(flattened_item)
        else:
            flattened_list.append(item)
    
    return flattened_list


def flatten_list_field(list_data: List[Dict[str, Any]]) -> List[str]:
    """
    Flatten DynamoDB list field (like sentiment, topics, speakers) to simple list of strings.
    
    Args:
        list_data: List of DynamoDB structures
        
    Returns:
        List of strings
    """
    flattened_list = []
    
    for item in list_data:
        if isinstance(item, dict):
            # Handle various DynamoDB list item structures
            flattened_item = dynamodb_attribute_to_python_type(item)
            if isinstance(flattened_item, dict) and 'S' in flattened_item:
                flattened_list.append(str(flattened_item['S']))
            elif isinstance(flattened_item, str):
                flattened_list.append(flattened_item)
            elif flattened_item:
                flattened_list.append(str(flattened_item))
        else:
            flattened_list.append(str(item))
    
    return flattened_list