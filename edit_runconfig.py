#!/usr/bin/env python3
"""
Script to read, edit, and save a modified copy of the TROPO run configuration file.
"""

import yaml
import os
from pathlib import Path

def load_runconfig(file_path):
    """Load the YAML run configuration file."""
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def modify_runconfig(config, input_file_path=None, output_dir=None, scratch_dir=None):
    """
    Modify specific paths in the run configuration.
    
    Args:
        config: The loaded YAML configuration
        input_file_path: New input file path
        output_dir: New output directory path
        scratch_dir: New scratch directory path
    """
    # Modify input file path if provided
    if input_file_path:
        config['RunConfig']['Groups']['PGE']['InputFilesGroup']['InputFilePaths'][0] = input_file_path
        config['RunConfig']['Groups']['SAS']['input_file']['input_file_path'] = input_file_path
    
    # Modify output directory paths if provided
    if output_dir:
        config['RunConfig']['Groups']['PGE']['ProductPathGroup']['OutputProductPath'] = output_dir
        config['RunConfig']['Groups']['SAS']['product_path_group']['product_path'] = output_dir
        config['RunConfig']['Groups']['SAS']['product_path_group']['sas_output_path'] = output_dir
    
    # Modify scratch directory paths if provided
    if scratch_dir:
        config['RunConfig']['Groups']['PGE']['ProductPathGroup']['ScratchPath'] = scratch_dir
        config['RunConfig']['Groups']['SAS']['product_path_group']['scratch_path'] = scratch_dir
    
    return config

def save_runconfig(config, output_path):
    """Save the modified configuration to a new file."""
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as file:
        yaml.dump(config, file, default_flow_style=False, sort_keys=False, indent=2)

def main():
    # File paths
    input_file = "tropo_sample_runconfig-v3.0.0-er.3.1.yaml"
    output_directory = "config"
    output_file = f"{output_directory}/modified_runconfig.yaml"
    
    # Load the original configuration
    print(f"Loading configuration from {input_file}...")
    config = load_runconfig(input_file)
    
    # Modify the configuration (example modifications)
    print("Modifying configuration...")
    modified_config = modify_runconfig(
        config,
        input_file_path="/home/ops/input_dir/my_input_file.nc",
        output_dir="/home/ops/my_output_dir",
        scratch_dir="/home/ops/my_scratch_dir"
    )
    
    # You can also modify other settings
    # Example: Change the number of workers
    modified_config['RunConfig']['Groups']['SAS']['worker_settings']['n_workers'] = 8
    
    # Example: Change the product version
    modified_config['RunConfig']['Groups']['PGE']['PrimaryExecutable']['ProductVersion'] = "0.3"
    modified_config['RunConfig']['Groups']['SAS']['product_path_group']['product_version'] = "0.3"
    
    # Save the modified configuration
    print(f"Saving modified configuration to {output_file}...")
    save_runconfig(modified_config, output_file)
    
    print("Configuration file successfully modified and saved!")
    print(f"Original file: {input_file}")
    print(f"Modified file: {output_file}")

if __name__ == "__main__":
    main() 