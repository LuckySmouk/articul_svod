import pandas as pd

# Чтение данных из Excel-файла
input_file = 'in.xlsx'
df = pd.read_excel(input_file)

# Удаление дубликатов, оставляем по одной строке для каждого дубликата
df_unique = df.drop_duplicates(keep='first')

# Сохранение результата в новый Excel-файл
output_file = 'table_in.xlsx'
df_unique.to_excel(output_file, index=False)

print(f"Обработано: {len(df)} строк входных данных.")
print(f"Осталось: {len(df_unique)} уникальных строк.")
print(f"Результат сохранён в '{output_file}'.")
