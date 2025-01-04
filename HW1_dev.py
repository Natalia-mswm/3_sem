import luigi
from luigi import ExternalTask
import subprocess
import luigi
from luigi import Task
import os
import gzip
import shutil
import pandas as pd
from io import StringIO
import luigi
from luigi import Task
import pandas as pd

class DownloadArchive(ExternalTask):
    dataset_name = luigi.Parameter()
    
    def output(self):
        download_dir = os.path.join(os.getcwd(), f"{self.dataset_name}")
        os.makedirs(download_dir, exist_ok=True)
        return luigi.LocalTarget(os.path.join(download_dir, f"{self.dataset_name}_archive.tar.gz"))
    
    def run(self):
        url_template = "https://www.ncbi.nlm.nih.gov/geo/download/?acc={}&format=file"
        url = url_template.format(self.dataset_name)
        
        download_dir = os.path.join(os.getcwd(), f"{self.dataset_name}")
        os.makedirs(download_dir, exist_ok=True)
        
        archive_path = os.path.join(download_dir, f"{self.dataset_name}_archive.tar.gz")
        
        with luigi.LocalTarget(archive_path).open('w') as f:
            subprocess.run(["wget", "-O", "-", url], stdout=f)

# Шаг 2
class ExtractAndSplit(Task):
    dataset_name = luigi.Parameter()
    
    def requires(self):
        return DownloadArchive(dataset_name=self.dataset_name)
    
    def output(self):
        return [
            luigi.LocalTarget(f"data/{dataset_name}/table_{key}.tsv") 
            for key in ["Probes", "Samples", "Design", "Platform"]
        ]
    
    def run(self):
        archive_path = self.input().path
        extract_dir = f"data/{self.dataset_name}"
        
        # Разархивировать архив
        with gzip.open(archive_path, 'rb') as f_in:
            with open(os.path.join(extract_dir, 'archive.tar'), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Распаковать архив
        with tarfile.open(os.path.join(extract_dir, 'archive.tar')) as tar:
            tar.extractall(path=extract_dir)
        
        # Создать словарь для хранения таблиц
        tables = {}
        
        for filename in os.listdir(extract_dir):
            if filename.endswith('.txt'):
                key = filename.split('.')[0]
                with open(os.path.join(extract_dir, filename), 'r') as f:
                    content = f.read()
                
                # Разделить таблицу на отдельные файлы
                dfs = self._split_table(content, key)
                
                for i, df in enumerate(dfs):
                    output_path = os.path.join(extract_dir, f"{key}_{i+1}.tsv")
                    df.to_csv(output_path, sep='\t', index=False)
        
        # Удалить исходный текстовый файл
        os.remove(os.path.join(extract_dir, 'GPL10558_HumanHT-12_V4_0_R1_15002873_B.txt'))


# Шаг 3
class ProcessProbes(Task):
    dataset_name = luigi.Parameter()
    
    def requires(self):
        return ExtractAndSplit(dataset_name=self.dataset_name)
    
    def output(self):
        return [
            luigi.LocalTarget(f"data/{self.dataset_name}/table_Probes.tsv"),
            luigi.LocalTarget(f"data/{self.dataset_name}/table_Probes_reduced.tsv")
        ]
    
    def run(self):
        probes_path = self.input().path
        
        # Загрузить таблицу Probes
        df = pd.read_csv(probes_path, sep='\t')
        
        # Сохранить полную таблицу
        df.to_csv(self.output()[0].path, index=False)
        
        # Создать урезанную версию таблицы
        reduced_df = df.drop(columns=['Definition', 'Ontology_Component', 
                                       'Ontology_Process', 'Ontology_Function', 
                                       'Synonyms', 'Obsolete_Probe_Id', 
                                       'Probe_Sequence'])
        
        reduced_df.to_csv(self.output()[1].path, index=False)


# Шаг 4
class CleanUp(Task):
    dataset_name = luigi.Parameter()
    
    def requires(self):
        return ProcessProbes(dataset_name=self.dataset_name)
    
    def run(self):
        extract_dir = f"data/{self.dataset_name}"
        os.remove(os.path.join(extract_dir, 'archive.tar'))
        os.rmdir(extract_dir)