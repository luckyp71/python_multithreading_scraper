import time
from yaml_reader import YamlPipelineExecutor
import os
from dotenv import load_dotenv

def main():
    load_dotenv('.env-local')
    pipeline_location = os.getenv('PIPELINE_LOCATION')
    if pipeline_location is None:
        print('Pipeline location is not defined')
        exit(1)
    scraper_start_time = time.time()

    yamlPipelineExecutor = YamlPipelineExecutor(pipeline_location)
    yamlPipelineExecutor.start()
    yamlPipelineExecutor.join()
    print("Extracting time took:", round(time.time() - scraper_start_time, 1))


if __name__ == '__main__':
    main()
