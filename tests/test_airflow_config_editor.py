# import unittest
# import os
# import yaml
# from dags.airflow_config_editor import create_modified_runconfig

# class TestAirflowConfigEditor(unittest.TestCase):

#     def setUp(self):
#         # Setup paths for the test
#         self.template_file = "tropo_sample_runconfig-v3.0.0-er.3.1.yaml"
#         self.output_file = "tests/modified_runconfig.yaml"

#         # Create a minimal template file for testing
#         self.template_content = {
#             'RunConfig': {
#                 'Groups': {
#                     'PGE': {
#                         'InputFilesGroup': {
#                             'InputFilePaths': ["/home/ops/input_dir/original.nc"]
#                         },
#                         'ProductPathGroup': {
#                             'OutputProductPath': "/home/ops/original_output_dir",
#                             'ScratchPath': "/home/ops/original_scratch_dir"
#                         },
#                         'PrimaryExecutable': {
#                             'ProductVersion': "0.1"
#                         }
#                     },
#                     'SAS': {
#                         'input_file': {
#                             'input_file_path': "/home/ops/input_dir/original.nc"
#                         },
#                         'product_path_group': {
#                             'product_path': "/home/ops/original_output_dir",
#                             'scratch_path': "/home/ops/original_scratch_dir",
#                             'sas_output_path': "/home/ops/original_output_dir",
#                             'product_version': "0.1"
#                         },
#                         'worker_settings': {
#                             'n_workers': 4
#                         }
#                     }
#                 }
#             }
#         }

#         # Write the template content to a file
#         with open(self.template_file, 'w') as file:
#             yaml.dump(self.template_content, file)

#     def tearDown(self):
#         # Clean up files after tests
#         if os.path.exists(self.output_file):
#             os.remove(self.output_file)
#         if os.path.exists(self.template_file):
#             os.remove(self.template_file)

#     def test_create_modified_runconfig(self):
#         # Call the function with test parameters
#         modified_path = create_modified_runconfig(
#             template_path=self.template_file,
#             output_path=self.output_file,
#             input_file="/home/ops/input_dir/test_input.nc",
#             output_dir="/home/ops/test_output_dir",
#             scratch_dir="/home/ops/test_scratch_dir",
#             n_workers=10,
#             product_version="0.5"
#         )

#         # Load the modified configuration
#         with open(modified_path, 'r') as file:
#             modified_config = yaml.safe_load(file)

#         # Assertions to check if the modifications are correct
#         self.assertEqual(modified_config['RunConfig']['Groups']['PGE']['InputFilesGroup']['InputFilePaths'][0], "/home/ops/input_dir/test_input.nc")
#         self.assertEqual(modified_config['RunConfig']['Groups']['PGE']['ProductPathGroup']['OutputProductPath'], "/home/ops/test_output_dir")
#         self.assertEqual(modified_config['RunConfig']['Groups']['PGE']['ProductPathGroup']['ScratchPath'], "/home/ops/test_scratch_dir")
#         self.assertEqual(modified_config['RunConfig']['Groups']['PGE']['PrimaryExecutable']['ProductVersion'], "0.5")
#         self.assertEqual(modified_config['RunConfig']['Groups']['SAS']['input_file']['input_file_path'], "/home/ops/input_dir/test_input.nc")
#         self.assertEqual(modified_config['RunConfig']['Groups']['SAS']['product_path_group']['product_path'], "/home/ops/test_output_dir")
#         self.assertEqual(modified_config['RunConfig']['Groups']['SAS']['product_path_group']['scratch_path'], "/home/ops/test_scratch_dir")
#         self.assertEqual(modified_config['RunConfig']['Groups']['SAS']['product_path_group']['sas_output_path'], "/home/ops/test_output_dir")
#         self.assertEqual(modified_config['RunConfig']['Groups']['SAS']['product_path_group']['product_version'], "0.5")
#         self.assertEqual(modified_config['RunConfig']['Groups']['SAS']['worker_settings']['n_workers'], 10)

# if __name__ == '__main__':
#     unittest.main() 