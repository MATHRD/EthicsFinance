import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import os
import re

CHEMIN = "C:/Users/mathy/OneDrive/Bureau/EthicsFinance" # A changer selon l'endroit où vous clonez le repo git
esg_grade = pd.read_csv(f'{CHEMIN}/tickers_finaux.csv')

start_date = "2022-01-01"
end_date = "2025-01-01"
output_file_price = f"{CHEMIN}/price1_ticker.csv"
output_file_volume = f"{CHEMIN}/volume_ticker.csv"
batch_size = 800
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
            return pd.DataFrame(), pd.DataFrame()

        combined_prices = pd.DataFrame()
        combined_volumes = pd.DataFrame()

        for ticker in batch:
            try:
                ticker_close = data[ticker][["Close"]].copy()
                ticker_close.rename(columns={"Close": ticker}, inplace=True)

                ticker_volume = data[ticker][["Volume"]].copy()
                ticker_volume.rename(columns={"Volume": ticker}, inplace=True)

                if combined_prices.empty:
                    combined_prices = ticker_close
                    combined_volumes = ticker_volume
                else:
                    combined_prices = combined_prices.join(ticker_close, how="outer")
                    combined_volumes = combined_volumes.join(ticker_volume, how="outer")

            except Exception as e:
                print(f"Erreur pour le ticker {ticker} : {e}")

        return combined_prices, combined_volumes

    except Exception as e:
        print(f"Erreur dans le téléchargement du batch : {e}")
        return pd.DataFrame(), pd.DataFrame()


# Boucle sur tous les tickers par batch de 100
df_all_prices = pd.DataFrame()
df_all_volumes = pd.DataFrame()

# Boucle sur tous les tickers par batch de 100
for i in range(0, len(tickers), batch_size):
    batch = tickers[i:i + batch_size]
    batch_prices, batch_volumes = download_batch(batch)

    if not batch_prices.empty:
        if df_all_prices.empty:
            df_all_prices = batch_prices
            df_all_volumes = batch_volumes
        else:
            df_all_prices = df_all_prices.join(batch_prices, how="outer")
            df_all_volumes = df_all_volumes.join(batch_volumes, how="outer")

    if i + batch_size < len(tickers):
        print("Pause de 2 minutes avant le prochain batch...")
        time.sleep(pause_duration)

print("✅ Téléchargement terminé.")
df_all_prices.reset_index(inplace=True)
df_all_volumes.reset_index(inplace=True)

# Sauvegarde
df_all_prices.to_csv(output_file_price, index=False)
df_all_volumes.to_csv(output_file_volume, index=False)

print(f"✅ Données enregistrées dans : prices_{output_file_price} et volumes_{output_file_volume}")
