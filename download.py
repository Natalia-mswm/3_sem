import os
import pandas as pd
from sklearn.datasets import load_iris

def download_data(output_path):
    
    # Загружаем набор данных Iris с помощью sklearn и сохраняем его в csv
    iris = load_iris()
    df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
    df['species'] = iris.target
    df.to_csv(output_path, index=False)
    print('The dataset has been loaded.')

# Загружаем и сохраняем данные
download_data('data/iris_dataset.csv')
