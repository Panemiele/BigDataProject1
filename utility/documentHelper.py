from random import randrange
import pandas as pd

nome_file = 'C:\\Users\\Gabri\\OneDrive\\Documenti\\Università\\BigData\\Progetti\\Progetto1\\Dataset\\Reviews.csv'
nome_nuovo_file = 'C:\\Users\\Gabri\\OneDrive\\Documenti\\Università\\BigData\\Progetti\\Progetto1\\Dataset\\CleanedDataset.csv'
nome_nuovo_file_doppia_dim = 'C:\\Users\\Gabri\\OneDrive\\Documenti\\Università\\BigData\\Progetti\\Progetto1\\Dataset\\CleanedDatasetDoubled.csv'
# leggi il file CSV in un DataFrame
df = pd.read_csv(nome_file)

# rimuovi le colonne 1 e 2 dal DataFrame
df = df.drop(['ProfileName', 'Summary'], axis=1)
# Verifica se il numeratore è maggiore del denominatore e, in caso positivo, modifica il DataFrame
df.loc[df['HelpfulnessNumerator'] > df['HelpfulnessDenominator'], 'HelpfulnessNumerator'] = df['HelpfulnessDenominator']

# Modificare il nome della colonna "Time" in "ReviewTime"
df.columns = df.columns.str.replace('Time', 'ReviewTime')

# Rimuovi tutte le virgole e caratteri speciali dalle recensioni
df["Text"] = df["Text"].replace(r'[^a-zA-Z0-9 ]+', ' ', regex=True)

# sovrascrivi il file CSV con il nuovo DataFrame
df.to_csv(nome_nuovo_file, index=False)

# Genera file di dimensione doppia
df.to_csv(nome_nuovo_file_doppia_dim, index=False)
df["Id"] = df["Id"]+568454
df["ProductId"] = df["ProductId"] + "Duplicated"
df["UserId"] = df["UserId"] + "Duplicated"
df["HelpfulnessDenominator"] = df["HelpfulnessDenominator"].apply(lambda x: x + 1)
df["HelpfulnessNumerator"] = df["HelpfulnessNumerator"].apply(lambda x: x + 1)
df["ReviewTime"] = randrange(1351209600+1)
df.loc[df['HelpfulnessNumerator'] > df['HelpfulnessDenominator'], 'HelpfulnessNumerator'] = df['HelpfulnessDenominator']
df.to_csv(nome_nuovo_file_doppia_dim, mode='a', index=False)
