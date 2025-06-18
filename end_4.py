import pandas as pd
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import threading
from typing import List, Dict, Optional, Set, Tuple
from collections import defaultdict

# Константы для настройки программы
CONFIG = {
    "INPUT_FILE": "in/all_out.xlsx",          # Путь к входному файлу
    "OUTPUT_FILE": "out/output_4.xlsx",         # Путь к выходному файлу
    "WORKERS": 10,                        # Количество параллельных потоков
    "ARTICUL_COLS": [                     # Колонки с артикулами
        "Доп. Артикул 1",
        "Доп. Артикул 2",
        "Доп. Артикул 3",
        "Доп. Артикул 4",
        "Доп. Артикул 5",
    ],
    "VTRAC_COLS": [                       # Колонки с VTRAC
        "vtrac_1",
        "vtrac_2",
        "vtrac_3",
        "vtrac_4",
        "vtrac_5",
    ],
    "NEW_COLUMN": "VTRAC",               # Название новой колонки
    "LOG_LEVEL": "DEBUG",                # Уровень логирования
    "MIN_PREFIX_LENGTH": 6,              # Минимальная длина префикса
}

# Настройка логгера
logger.remove()
logger.add(
    sink=sys.stdout,
    level=CONFIG["LOG_LEVEL"],
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{thread.name}</cyan> | <level>{message}</level>"
)


class VtracProcessor:
    def __init__(self):
        self.df = None
        self.articul_to_vtrac = None
        self.lock = threading.Lock()

    def load_data(self) -> None:
        """Загрузка данных из Excel файла"""
        logger.info(f"Загрузка данных из файла {CONFIG['INPUT_FILE']}")
        try:
            self.df = pd.read_excel(CONFIG["INPUT_FILE"])
            logger.success(
                f"Данные успешно загружены. Всего строк: {len(self.df)}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла: {e}")
            raise

    def save_data(self) -> None:
        """Сохранение данных в Excel файл"""
        logger.info(f"Сохранение результатов в файл {CONFIG['OUTPUT_FILE']}")
        try:
            self.df.to_excel(CONFIG["OUTPUT_FILE"], index=False)
            logger.success("Результаты успешно сохранены")
        except Exception as e:
            logger.error(f"Ошибка при сохранении файла: {e}")
            raise

    def prepare_articul_mapping(self) -> None:
        """Создание маппинга артикулов к vtrac значениям"""
        logger.info("Подготовка маппинга артикулов к VTRAC...")
        self.articul_to_vtrac = {}

        # Берем только строки, где есть хотя бы одно значение vtrac
        vtrac_rows = self.df[self.df[CONFIG["VTRAC_COLS"]].notna().any(axis=1)]

        for _, row in vtrac_rows.iterrows():
            articuls = set(str(art)
                           for art in row[CONFIG["ARTICUL_COLS"]] if pd.notna(art))
            vtracs = [str(v) for v in row[CONFIG["VTRAC_COLS"]] if pd.notna(v)]

            for art in articuls:
                if art not in self.articul_to_vtrac:
                    self.articul_to_vtrac[art] = set()
                self.articul_to_vtrac[art].update(vtracs)

        logger.debug(
            f"Создан маппинг для {len(self.articul_to_vtrac)} уникальных артикулов")

    def process_vtrac_matching(self, start_idx: int, end_idx: int) -> Dict[int, List[str]]:
        """Обработка совпадений артикулов для диапазона строк"""
        results = {}
        logger.debug(f"Обработка строк с {start_idx} по {end_idx}")

        for idx in range(start_idx, end_idx):
            if idx >= len(self.df):
                break

            # Проверяем, есть ли в строке vtrac значения
            if self.df.loc[idx, CONFIG["VTRAC_COLS"]].notna().any():
                continue

            # Получаем все артикулы в строке
            current_articuls = set(
                str(art) for art in self.df.loc[idx, CONFIG["ARTICUL_COLS"]]
                if pd.notna(art))

            # Ищем совпадения артикулов
            found_vtracs = set()
            for art in current_articuls:
                if art in self.articul_to_vtrac:
                    found_vtracs.update(self.articul_to_vtrac[art])

            if found_vtracs:
                results[idx] = list(found_vtracs)
                logger.debug(
                    f"Найдены совпадения для строки {idx}: {found_vtracs}")

        return results

    def apply_vtrac_matches(self, matches: Dict[int, List[str]]) -> None:
        """Применение найденных совпадений vtrac к DataFrame"""
        with self.lock:
            for idx, vtracs in matches.items():
                # Заполняем vtrac колонки найденными значениями
                for i, vtrac in enumerate(vtracs[:len(CONFIG["VTRAC_COLS"])]):
                    self.df.at[idx, CONFIG["VTRAC_COLS"][i]] = vtrac
                logger.debug(f"Обновлены VTRAC для строки {idx}")


    def find_common_vtrac(self, vtrac_values: List[str]) -> Optional[str]:
        """Нахождение общего префикса для vtrac значений"""
        if not vtrac_values or all(pd.isna(v) for v in vtrac_values):
            return None

        # Фильтруем None значения и приводим к строкам
        str_values = [str(v) for v in vtrac_values if pd.notna(v)]
        if len(str_values) < 2:
            return str_values[0] if str_values else None

        # Начинаем с максимальной длины префикса (8 символов)
        max_prefix_length = 8
        min_prefix_length = CONFIG["MIN_PREFIX_LENGTH"]

        for prefix_length in range(max_prefix_length, min_prefix_length - 1, -1):
            prefix_groups = defaultdict(list)
            for s in str_values:
                if len(s) >= prefix_length:
                    prefix = s[:prefix_length]
                    prefix_groups[prefix].append(s)

            # Находим самую большую группу
            largest_group = max(prefix_groups.values(), key=len, default=[])

            # Если в группе больше одного элемента, возвращаем общий префикс
            if len(largest_group) > 1:
                common_prefix = largest_group[0][:prefix_length]
                return common_prefix

        # Если не нашли общий префикс, возвращаем None
        return None


    def process_common_vtrac(self, start_idx: int, end_idx: int) -> Dict[int, str]:
        """Обработка общего VTRAC для диапазона строк"""
        results = {}
        logger.debug(
            f"Поиск общего VTRAC для строк с {start_idx} по {end_idx}")

        for idx in range(start_idx, end_idx):
            if idx >= len(self.df):
                break

            vtrac_values = self.df.loc[idx, CONFIG["VTRAC_COLS"]].tolist()
            common_vtrac = self.find_common_vtrac(vtrac_values)

            if common_vtrac:
                results[idx] = common_vtrac
                logger.debug(
                    f"Найден общий VTRAC для строки {idx}: {common_vtrac}")

        return results

    def apply_common_vtrac(self, common_vtracs: Dict[int, str]) -> None:
        """Применение общего VTRAC к DataFrame"""
        with self.lock:
            for idx, common_vtrac in common_vtracs.items():
                self.df.at[idx, CONFIG["NEW_COLUMN"]] = common_vtrac
                logger.debug(f"Записан общий VTRAC для строки {idx}")

    def run_parallel_processing(self, task_func, apply_func) -> None:
        """Запуск обработки в параллельных потоках"""
        chunk_size = max(1, len(self.df) // CONFIG["WORKERS"])
        futures = []

        with ThreadPoolExecutor(max_workers=CONFIG["WORKERS"], thread_name_prefix="Worker") as executor:
            # Разделяем DataFrame на чанки для обработки
            for i in range(0, len(self.df), chunk_size):
                future = executor.submit(
                    task_func, i, min(i + chunk_size, len(self.df)))
                futures.append(future)
                logger.debug(
                    f"Запущен поток для обработки строк {i}-{min(i + chunk_size, len(self.df))}")

            # Собираем результаты по мере выполнения
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        apply_func(result)
                except Exception as e:
                    logger.error(f"Ошибка в потоке: {e}")

        logger.success("Параллельная обработка завершена")

    def process(self) -> None:
        """Основной метод обработки данных"""
        try:
            # Шаг 1: Загрузка данных
            self.load_data()

            # Шаг 2: Подготовка маппинга артикулов к vtrac
            self.prepare_articul_mapping()

            # Шаг 3: Задача 1 - Поиск и заполнение vtrac по артикулам
            logger.info("Начало выполнения задачи 1...")
            self.run_parallel_processing(
                self.process_vtrac_matching,
                self.apply_vtrac_matches
            )
            logger.success("Задача 1 выполнена")

            # Шаг 4: Задача 2 - Нахождение общего VTRAC
            logger.info("Начало выполнения задачи 2...")
            self.run_parallel_processing(
                self.process_common_vtrac,
                self.apply_common_vtrac
            )
            logger.success("Задача 2 выполнена")

            # Шаг 5: Сохранение результатов
            self.save_data()

        except Exception as e:
            logger.critical(f"Критическая ошибка: {e}")
            raise


if __name__ == "__main__":
    logger.info("Запуск программы...")
    processor = VtracProcessor()
    processor.process()
    logger.success("Программа успешно завершена")
