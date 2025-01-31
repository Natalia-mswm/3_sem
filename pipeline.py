from clearml import PipelineDecorator, Task
import subprocess
import os

@PipelineDecorator.component(cache=False, execution_queue="default")
def dvc_pull():
    subprocess.run(["dvc", "pull"], check=True)

@PipelineDecorator.component(cache=False, execution_queue="default")
def download_data():
    subprocess.run(["python", "scripts/download.py"], check=True)

@PipelineDecorator.component(cache=False, execution_queue="default")
def process_data():
    subprocess.run(["python", "scripts/process_data.py"], check=True)

@PipelineDecorator.component(execution_queue="default")
def train_model_1():
    try:
        current_task = Task.current_task()
        if current_task:
            task_id = current_task.id
            print(f"Current task ID in train_model_1: {task_id}")
            
            # Создание новой задачи для обучения модели
            new_task = Task.init(project_name="my_mlops_project", task_name="train_with_clearml_1")
            new_task.execute_remotely(queue_name='default')  # Запускаем задачу

            # Логирование
            print("Starting training for model 1...")
            
            # Запускаем скрипт с помощью subprocess
            subprocess.run(["python", "scripts/train.py", "--model-type", "model_1"], check=True)

        else:
            print("Current task is None in train_model_1")
            raise ValueError("Current task is None in train_model_1")
    except Exception as e:
        print(f"Error in train_model_1 component: {e}")
        raise

@PipelineDecorator.component(execution_queue="default")
def train_model_2():
    try:
        current_task = Task.current_task()
        if current_task:
            task_id = current_task.id
            print(f"Current task ID in train_model_2: {task_id}")
            
            # Создание новой задачи для обучения модели
            new_task = Task.init(project_name="my_mlops_project", task_name="train_with_clearml_2")
            new_task.execute_remotely(queue_name='default')  # Запускаем задачу

            # Логирование
            print("Starting training for model 2...")
            
            # Запускаем скрипт с помощью subprocess
            subprocess.run(["python", "scripts/train.py", "--model-type", "model_2"], check=True)

        else:
            print("Current task is None in train_model_2")
            raise ValueError("Current task is None in train_model_2")
    except Exception as e:
        print(f"Error in train_model_2 component: {e}")
        raise

@PipelineDecorator.component(cache=False, execution_queue="default")
def dvc_repro():
    subprocess.run(["dvc", "repro"], check=True)

@PipelineDecorator.component(cache=False, execution_queue="default")
def dvc_push():
    subprocess.run(["dvc", "push"], check=True)

@PipelineDecorator.pipeline(
    name='mlops_pipeline',
    project='my_mlops_project',
    version='0.1'
)
def mlops_pipeline_logic():
  dvc_pull()
  download_data()
  process_data()
  train_model_1()
  train_model_2()
  dvc_repro()
  dvc_push()

if __name__ == '__main__':
    # run the pipeline on the current machine, for local debugging
    # for scale-out, comment-out the following line (Make sure a
    # 'services' queue is available and serviced by a ClearML agent
    # running either in services mode or through K8S/Autoscaler)
    PipelineDecorator.run_locally()
    mlops_pipeline_logic()