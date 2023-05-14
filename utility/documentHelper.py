import pandas as pd

nome_file = 'C:\\Users\\Gabri\\OneDrive\\Documenti\\Università\\BigData\\Progetti\\Progetto1\\Dataset\\Reviews.csv'
nome_nuovo_file = 'C:\\Users\\Gabri\\OneDrive\\Documenti\\Università\\BigData\\Progetti\\Progetto1\\Dataset\\CleanedDataset.csv'
# leggi il file CSV in un DataFrame
df = pd.read_csv(nome_file)

# rimuovi le colonne 1 e 2 dal DataFrame
df = df.drop(['ProfileName', 'Summary'], axis=1)
# Verifica se il numeratore è maggiore del denominatore e, in caso positivo, modifica il DataFrame
df.loc[df['HelpfulnessNumerator'] > df['HelpfulnessDenominator'], 'HelpfulnessNumerator'] = df['HelpfulnessDenominator']

# Modificare il nome della colonna "Time" in "ReviewTime"
df.columns = df.columns.str.replace('Time', 'ReviewTime')

# sovrascrivi il file CSV con il nuovo DataFrame
df.to_csv(nome_nuovo_file, index=False)