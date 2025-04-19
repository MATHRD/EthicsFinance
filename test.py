import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import os
import re

CHEMIN = "C:/Users/mathy/OneDrive/Bureau/EthicsFinance" # A changer selon l'endroit où vous clonez le repo git
esg_grade = pd.read_csv(f'{CHEMIN}/Score ESG brut.csv')

esg_grade = esg_grade.dropna(subset=['ISSUER_EQUITY_TICKER', 'Groupe Industrie GICS', 'Score ESG brut'])
esg_grade = esg_grade.drop(columns=['Pays', 'Date de Scoring', 'Identifiant Bloomberg Company', 'Identifiant sous industrie GICS', "Nom de l'émetteur"])
esg_grade = esg_grade[~esg_grade["ISSUER_EQUITY_TICKER"].str.contains(r'\d')]
esg_grade["ISSUER_EQUITY_TICKER"] = esg_grade["ISSUER_EQUITY_TICKER"].str.split().str[0]


top_60_percent = esg_grade.sort_values("Score ESG brut", ascending=False)
top_60_percent = top_60_percent.head(int(0.6 * len(esg_grade)))


start_date = "2022-01-01"
end_date = "2025-01-01"
output_file = f"{CHEMIN}/price_ticker.csv"
batch_size = 200
pause_duration = 120

# Récupérer tous les tickers uniques
tickers = esg_grade["ISSUER_EQUITY_TICKER"].dropna().unique().tolist()

df_all_prices = pd.DataFrame()

# Fonction pour télécharger un lot de tickers
def download_batch(batch):
    try:
        print(f"Téléchargement d'un batch de {len(batch)} tickers...")
        data = yf.download(batch, start=start_date, end=end_date, interval="1d", group_by="ticker", auto_adjust=False, threads=True)

        if data.empty:
            print("Aucune donnée récupérée pour ce batch.")
            return pd.DataFrame()

        combined = pd.DataFrame()
        for ticker in batch:
            try:
                ticker_data = data[ticker][["Close"]].copy()
                ticker_data.rename(columns={"Close": ticker}, inplace=True)
                if combined.empty:
                    combined = ticker_data
                else:
                    combined = combined.join(ticker_data, how="outer")
            except Exception as e:
                print(f"Erreur pour le ticker {ticker} : {e}")

        return combined

    except Exception as e:
        print(f"Erreur dans le téléchargement du batch : {e}")
        return pd.DataFrame()

# Boucle sur tous les tickers par batch de 100
for i in range(0, len(tickers), batch_size):
    batch = tickers[i:i + batch_size]
    batch_df = download_batch(batch)

    if not batch_df.empty:
        if df_all_prices.empty:
            df_all_prices = batch_df
        else:
            df_all_prices = df_all_prices.join(batch_df, how="outer")

    if i + batch_size < len(tickers):
        print("Pause de 2 minutes avant le prochain batch...")
        time.sleep(pause_duration)

print("✅ Téléchargement terminé.")
df_all_prices.reset_index(inplace=True)

df_all_prices.to_csv(output_file, index=False)
print(f"✅ Téléchargement terminé. Données enregistrées dans : {output_file}")