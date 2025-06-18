import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor


def clean_art(art):
    # Удаление пробелов, завершающих недопустимых символов и преобразование к верхнему регистру
    return re.sub(r'[-/\s]+$', '', art).replace(' ', '').upper()


def is_valid_art(art):
    return len(art) >= 4 and ' ' not in art


def process_articul(articul, original_art):
    variants = set()
    if pd.isna(articul):
        return []
    articul = str(articul).strip().upper()
    articul = clean_art(articul)

    # Исключаем исходный артикул
    if articul != original_art:
        variants.add(articul)

    # Обработка дефисов
    if '-' in articul:
        # Часть до последнего дефиса
        part_before_last_dash = articul.rsplit('-', 1)[0]
        if is_valid_art(part_before_last_dash):
            variants.add(clean_art(part_before_last_dash))
            # Удаление последних двух символов
            if len(part_before_last_dash) >= 7:
                truncated = clean_art(part_before_last_dash[:-2])
                if is_valid_art(truncated):
                    variants.add(truncated)

        # Разделение на первые две части
        parts = articul.split('-', 2)
        if len(parts) >= 2:
            first_two = '-'.join(parts[:2])
            if is_valid_art(first_two):
                variants.add(clean_art(first_two))
            if len(parts) >= 3:
                third_part = parts[2]
                if is_valid_art(third_part):
                    variants.add(clean_art(third_part))

    # Обработка слешей
    if '/' in articul:
        # Замены слешей
        replaced_with_dash = clean_art(articul.replace('/', '-'))
        if is_valid_art(replaced_with_dash):
            variants.add(replaced_with_dash)
        replaced_with_space = clean_art(articul.replace('/', ' / '))
        if is_valid_art(replaced_with_space):
            variants.add(replaced_with_space)
        replaced_with_hyphen = clean_art(articul.replace('/', ' - '))
        if is_valid_art(replaced_with_hyphen):
            variants.add(replaced_with_hyphen)

        # Разделение по слешу
        parts = articul.split('/')
        for part in parts:
            cleaned = clean_art(part)
            if is_valid_art(cleaned):
                variants.add(cleaned)

    # Фильтрация по длине и чистка
    valid_variants = {v for v in variants if is_valid_art(v)}
    return list(valid_variants)


def extract_from_nomenclature(nomenclature, original_art):
    if pd.isna(nomenclature):
        return []
    articuls = []
    nomenclature = str(nomenclature).upper()

    # Извлечение из скобок
    bracket_pattern = r'\(([^)]*?)\)'
    for match in re.findall(bracket_pattern, nomenclature):
        # Разделяем по слешам и точкам с запятой, затем очищаем каждую часть
        parts = re.split(r'[/;]', match)
        for part in parts:
            # Удаляем лишние пробелы и извлекаем первый "словообразный" артикул
            cleaned_part = part.strip()
            # Извлекаем первый подходящий артикул (последовательность букв и цифр без пробелов)
            art_match = re.search(r'([A-Z0-9-]+)', cleaned_part)
            if art_match:
                art = clean_art(art_match.group(1))
                if is_valid_art(art) and art != original_art:
                    articuls.append(art)

    # Извлечение после "арт."
    art_pattern = r'арт\.?\s*([^;$]*)'
    for match in re.findall(art_pattern, nomenclature, re.IGNORECASE):
        cleaned = clean_art(match.split('/')[0].split('(')[0].split(' ')[0])
        if is_valid_art(cleaned) and cleaned != original_art:
            articuls.append(cleaned)

    return articuls


def process_row(row):
    articul = row.get('Артикул', '')
    nomenclature = row.get('Номенклатура', '')
    original_art = clean_art(str(articul)) if not pd.isna(articul) else ''

    all_articuls = set()
    if articul:
        processed = process_articul(articul, original_art)
        for art in processed:
            if art != original_art:
                all_articuls.add(art)

    nomenclature_articuls = extract_from_nomenclature(
        nomenclature, original_art)
    for art in nomenclature_articuls:
        if art != original_art:
            all_articuls.add(art)

    # Удаление дубликатов и сортировка
    unique_arts = sorted(list(all_articuls), key=lambda x: (-len(x), x))
    return unique_arts


def main():
    input_file = 'tabl_in.xlsx'
    output_file = 'tabl_out2.xlsx'

    df = pd.read_excel(input_file, sheet_name=0)
    rows = df.to_dict('records')

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(process_row, rows))

    max_articuls = max(len(arts) for arts in results) if results else 0
    for i in range(max_articuls):
        df[f'Доп. Артикул {i+1}'] = ''

    for idx, arts in enumerate(results):
        for i, art in enumerate(arts):
            df.at[idx, f'Доп. Артикул {i+1}'] = art

    df.to_excel(output_file, index=False)
    print(f"Обработка завершена. Результат сохранен в {output_file}")


if __name__ == "__main__":
    main()
