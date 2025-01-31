import pandas as pd
from sklearn.preprocessing import StandardScaler

def load_data(input_path):
    # Загрузить данные
    df = pd.read_csv(input_path)
    return df

def clean_data(df):
    # Удаляем пропущенные значения, если есть
    df = df.dropna()
    return df

def scale_features(df):
    # Стандартизация признаков (кроме колонки 'species')
    scaler = StandardScaler()
    feature_columns = df.columns[:-1]
    df[feature_columns] = scaler.fit_transform(df[feature_columns])
    return df

def create_target(df):
     # Преобразуем колонку species в колонку target
    df = df.rename(columns={'species':'target'})
    return df

def save_data(df, output_path):
    # Сохранить обработанные данные
    df.to_csv(output_path, index=False)

def main(input_path, output_path):
    # Основной процесс обработки данных
    df = load_data(input_path)
    df = clean_data(df)
    df = scale_features(df)
    df = create_target(df)
    save_data(df, output_path)

if __name__ == "__main__":
    main("data/iris_dataset.csv", "data/proc_iris_dataset.csv")
