import os
import asyncio
import logging
import time
import pandas as pd
import shutil  # Для перемещения файлов в архив
import csv

from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler



# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("s3_client.log"),  # Запись логов в файл
        logging.StreamHandler()  # Вывод логов в консоль
    ]
)

logger = logging.getLogger("S3")  # Создание логгера с именем "S3"

load_dotenv()  # Загрузка переменных окружения из файла .env

# Конфигурация приложения
CONFIG = {
    "key_id": os.getenv("KEY_ID"),
    "secret": os.getenv("SECRET"),
    "endpoint": os.getenv("ENDPOINT"),
    "container": os.getenv("CONTAINER"),
    "local_folder": "local_folder",  # Папка для отслеживания изменений
    "archive_folder": "archive_folder",  # Папка для архивирования обработанных файлов
}

# Создаем необходимые папки (если их нет)
Path(CONFIG["local_folder"]).mkdir(parents=True, exist_ok=True)
Path(CONFIG["archive_folder"]).mkdir(parents=True, exist_ok=True)

def format_size(size_bytes: int) -> str:
    """Форматирует размер файла в удобочитаемый вид."""

    if size_bytes < 1024:
        return f"{size_bytes} bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


class AsyncObjectStorage:
    """Класс для асинхронного взаимодействия с объектным хранилищем."""


    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        """
        Инициализация объекта AsyncObjectStorage.

        Args:
            key_id: ID ключа доступа.
            secret: Секретный ключ доступа.
            endpoint: URL эндпоинта.
            container: Имя контейнера (бакета).
        """
        if not all([key_id, secret, endpoint, container]):
            raise ValueError("Missing required configuration parameters")

        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
            "verify": False,  # Отключение проверки SSL (только для разработки!)
        }
        self._bucket = container
        self._session = get_session()  # Получение асинхронной сессии aiobotocore


    @asynccontextmanager
    async def _connect(self):
        """Асинхронный контекстный менеджер для подключения к S3."""

        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection



    async def send_file(self, local_source: str, target_name: Optional[str] = None) -> None:
        """
        Загружает файл из локальной файловой системы в бакет ОХ.

        Args:
            local_source: Путь к локальному файлу.
            target_name: Имя файла в объектном хранилище (если не указано, используется имя локального файла).
        """

        try:
            async with self._connect() as remote:  # Соединение с объектным хранилищем
                with open(local_source, 'rb') as f:  # Открытие файла в бинарном режиме для чтения
                    binary_data = f.read()  # Чтение содержимого файла
                if target_name is None:
                    target_name = Path(local_source).name  # Использование имени локального файла, если target_name не указан

                await remote.put_object(  # Асинхронная загрузка в объектное хранилище
                    Bucket=self._bucket,  # Имя контейнера (бакета)
                    Key=target_name,  # Ключ (имя) файла в объектном хранилище
                    Body=binary_data  # Тело файла (бинарные данные)
                )
                logger.info(f"Sent: {target_name}")  # Логирование успешной отправки файла

        except ClientError as error:
            logger.error(f"Failed to send {target_name}: {error}")  # Логирование ошибки при отправке файла


# 1. Генерация CSV файлов
async def generate_csv(folder: str = CONFIG["local_folder"], interval: int = 5):
    """Генерирует CSV файлы каждые 'interval' секунд."""

    while True:
        filename = Path(folder) / f"data_{int(time.time())}.csv"  # Именование файла на основе текущего времени
        try:
            with open(filename, 'w', newline='') as csvfile:  # Открытие файла для записи в режиме CSV
                writer = csv.writer(csvfile)  # Создание объекта записи CSV
                writer.writerow(['timestamp', 'value'])  # Запись заголовка
                writer.writerow([time.time(), time.monotonic()]) #Запись данных
            logger.info(f"Generated {filename}")  # Логирование успешной генерации файла
        except Exception as e:
            logger.error(f"Error generating CSV: {e}")  # Логирование ошибки при генерации файла
        await asyncio.sleep(interval)  # Асинхронная приостановка на 'interval' секунд


# 3, 4, 5.  Обработка CSV, загрузка, архивирование
async def process_csv(file_path: str, storage: AsyncObjectStorage):
    """Обрабатывает CSV файл: фильтрует, сохраняет, загружает и архивирует."""

    logger.info(f"Processing file: {file_path}")  # Логирование начала обработки файла

    try:
        # 3. Чтение, фильтрация, сохранение
        df = pd.read_csv(file_path)  # Чтение CSV файла в DataFrame
        filtered_df = df[df['value'] > 0]  # Фильтрация: отбор строк, где значение в столбце 'value' больше 0
        temp_file = "temp_filtered.csv"  # Имя временного файла для сохранения отфильтрованных данных
        filtered_df.to_csv(temp_file, index=False)  # Сохранение отфильтрованных данных во временный файл
        logger.info(f"File {file_path} filtered and saved to {temp_file}")  # Логирование успешной фильтрации и сохранения

        # 4. Загрузка отфильтрованного файла и логов
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        s3_filename = f"temp_filtered_{timestamp}.csv"
        await storage.send_file(temp_file, s3_filename)  # Загрузка отфильтрованного файла в ОХ
        await storage.send_file("s3_client.log", "s3_client.log")  # Загрузка файла логов в ОХ

        # 5. Архивация оригинального файла
        archive_path = Path(CONFIG["archive_folder"]) / Path(file_path).name  # Формирование пути для архивации файла
        shutil.move(file_path, archive_path)  # Перемещение оригинального файла в папку архива
        logger.info(f"Moved {file_path} to {archive_path}")  # Логирование успешной архивации файла
        os.remove(temp_file)
        logger.info(f"Deleted {temp_file}") # Логирование удаления временного файла

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")  # Логирование ошибки при обработке файла


# 2. Обработчик событий Watchdog
class CSVHandler(FileSystemEventHandler):
    """Обработчик событий файловой системы для отслеживания создания CSV файлов."""
    def __init__(self, storage: AsyncObjectStorage, loop: asyncio.AbstractEventLoop):
        """
        Инициализация объекта CSVHandler.

        Args:
            storage: Объект AsyncObjectStorage для загрузки файлов.
            loop: Асинхронный event loop.
        """
        self.storage = storage  # Сохранение ссылки на объект AsyncObjectStorage
        self.loop = loop  # Сохранение ссылки на асинхронный event loop

    def on_created(self, event):
        """Обработчик события создания файла."""
        if event.is_directory:  # Игнорирование событий создания директорий
            return
        if event.src_path.endswith(".csv"):  # Обработка только CSV файлов
            # Планирование задачи в главном event loop
            self.loop.call_soon_threadsafe(asyncio.create_task, process_csv(event.src_path, self.storage))


async def main():
    """Главная асинхронная функция."""

    # Создание объекта AsyncObjectStorage для взаимодействия с объектным хранилищем
    storage = AsyncObjectStorage(
        key_id=CONFIG["key_id"],
        secret=CONFIG["secret"],
        endpoint=CONFIG["endpoint"],
        container=CONFIG["container"]
    )

    # Получение текущего event loop
    loop = asyncio.get_event_loop()

    # Запуск генерации CSV файлов
    asyncio.create_task(generate_csv())  # Создание асинхронной задачи для генерации CSV файлов

    # Запуск Watchdog observer
    event_handler = CSVHandler(storage, loop)  # Создание обработчика событий файловой системы
    observer = Observer()  # Создание объекта Watchdog observer
    observer.schedule(event_handler, CONFIG["local_folder"], recursive=False)  # Назначение обработчика событий для отслеживаемой папки
    observer.start()  # Запуск отслеживания

    try:
        while True:
            await asyncio.sleep(1)  # Бесконечный цикл с асинхронной приостановкой на 1 секунду
    except KeyboardInterrupt:
        observer.stop()  # Остановка observer при получении сигнала прерывания (Ctrl+C)
    observer.join()  # Ожидание завершения работы observer


if __name__ == "__main__":
    asyncio.run(main())  # Запуск главной асинхронной функции
