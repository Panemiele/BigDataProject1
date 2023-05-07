import pandas as pd

nome_file = 'C:\\Users\\Gabri\\OneDrive\\Documenti\\Università\\BigData\\Progetti\\Progetto1\\Dataset\\CleanedDataset.csv'
# leggi il file CSV in un DataFrame
df = pd.read_csv(nome_file)

# rimuovi le colonne 1 e 2 dal DataFrame
df = df.drop(['ProfileName', 'Summary'], axis=1)

# sovrascrivi il file CSV con il nuovo DataFrame
df.to_csv(nome_file, index=False)