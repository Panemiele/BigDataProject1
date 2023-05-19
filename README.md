# BigDataProject1
L'obiettivo principale del progetto è l'analisi del dataset Amazon Fine Food Reviews disponibile su Kaggle. Questo dataset contiene circa 500.000 recensioni di prodotti gastronomici pubblicate su Amazon dal 1999 al 2012. 

<br>

Dataset: https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews

<br>

Specifiche dei 3 job:

•	Generazione dei 10 prodotti con il maggior numero di recensioni per ogni anno, insieme alle 5 parole più frequenti (con almeno 4 caratteri) utilizzate nelle recensioni relative a ciascun prodotto.

•	Creazione di una lista ordinata di utenti in base al loro apprezzamento, calcolato come la media dell'utilità delle recensioni che hanno scritto. L'utilità è definita dal rapporto tra il numero di utenti che hanno trovato la recensione utile e il numero totale di utenti che hanno valutato la recensione.

•	Generazione di gruppi di utenti con gusti affini. Gli utenti vengono considerati con gusti affini se hanno recensito almeno tre prodotti in comune con un punteggio uguale o superiore a 4. L'output dell'applicazione include i prodotti condivisi da ciascun gruppo e l'ordinamento è basato sull'identificatore dell'utente del primo elemento del gruppo.

<br><br>

Ogni applicazione viene implementata utilizzando 4 diverse tecnologie:
- MapReduce
- Hive
- Spark Core
- SparkSQL
