#!/usr/bin/env python3
"""
Generates data/fraud_transactions.min.csv.gz, a small simulated transaction
dataset with the generator from the "Machine Learning for Credit Card Fraud
Detection" handbook (chapter 3.2), scaled down so the whole sample runs in a
few minutes: 50 customers, 50 terminals and 122 days from 2019-04-01.

https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html

Every random draw is seeded, so a rerun with the same numpy and pandas
versions produces the same rows.
"""

import os
import random

import numpy as np
import pandas as pd

N_CUSTOMERS = 50
N_TERMINALS = 50
RADIUS = 25
START_DATE = "2019-04-01"
NB_DAYS = 122
OUTPUT = os.path.join(os.path.dirname(__file__), "fraud_transactions.min.csv.gz")


def generate_customer_profiles(n_customers, random_state=0):
    np.random.seed(random_state)
    rows = []
    for customer_id in range(n_customers):
        x, y = np.random.uniform(0, 100), np.random.uniform(0, 100)
        mean_amount = np.random.uniform(5, 100)
        std_amount = mean_amount / 2
        mean_nb_tx_per_day = np.random.uniform(0, 4)
        rows.append([customer_id, x, y, mean_amount, std_amount, mean_nb_tx_per_day])
    return pd.DataFrame(
        rows,
        columns=[
            "CUSTOMER_ID",
            "x_customer_id",
            "y_customer_id",
            "mean_amount",
            "std_amount",
            "mean_nb_tx_per_day",
        ],
    )


def generate_terminal_profiles(n_terminals, random_state=0):
    np.random.seed(random_state)
    rows = [
        [terminal_id, np.random.uniform(0, 100), np.random.uniform(0, 100)]
        for terminal_id in range(n_terminals)
    ]
    return pd.DataFrame(rows, columns=["TERMINAL_ID", "x_terminal_id", "y_terminal_id"])


def terminals_within_radius(customer, x_y_terminals, r):
    x_y_customer = customer[["x_customer_id", "y_customer_id"]].values.astype(float)
    dist = np.sqrt(np.sum(np.square(x_y_customer - x_y_terminals), axis=1))
    return list(np.where(dist < r)[0])


def generate_transactions(customer, start_date, nb_days):
    random.seed(int(customer.CUSTOMER_ID))
    np.random.seed(int(customer.CUSTOMER_ID))
    rows = []
    for day in range(nb_days):
        nb_tx = np.random.poisson(customer.mean_nb_tx_per_day)
        for _ in range(nb_tx):
            time_tx = int(np.random.normal(86400 / 2, 20000))
            if 0 < time_tx < 86400:
                amount = np.random.normal(customer.mean_amount, customer.std_amount)
                if amount < 0:
                    amount = np.random.uniform(0, customer.mean_amount * 2)
                amount = np.round(amount, decimals=2)
                if len(customer.available_terminals) > 0:
                    terminal_id = random.choice(customer.available_terminals)
                    rows.append(
                        [time_tx + day * 86400, day, customer.CUSTOMER_ID, terminal_id, amount]
                    )
    df = pd.DataFrame(
        rows,
        columns=["TX_TIME_SECONDS", "TX_TIME_DAYS", "CUSTOMER_ID", "TERMINAL_ID", "TX_AMOUNT"],
    )
    if len(df) > 0:
        df["TX_DATETIME"] = pd.to_datetime(df["TX_TIME_SECONDS"], unit="s", origin=start_date)
    return df


def add_frauds(customers, terminals, transactions):
    transactions["TX_FRAUD"] = 0
    transactions["TX_FRAUD_SCENARIO"] = 0

    # Scenario 1: any amount above 220 is a fraud
    transactions.loc[transactions.TX_AMOUNT > 220, ["TX_FRAUD", "TX_FRAUD_SCENARIO"]] = [1, 1]

    # Scenario 2: every 10 days one terminal is compromised for the next 28 days
    for day in range(0, transactions.TX_TIME_DAYS.max(), 10):
        compromised = terminals.TERMINAL_ID.sample(n=1, random_state=day)
        hit = transactions[
            (transactions.TX_TIME_DAYS >= day)
            & (transactions.TX_TIME_DAYS < day + 28)
            & (transactions.TERMINAL_ID.isin(compromised))
        ]
        transactions.loc[hit.index, ["TX_FRAUD", "TX_FRAUD_SCENARIO"]] = [1, 2]

    # Scenario 3: every 7 days one customer's card is compromised for 14 days,
    # and a third of its transactions are frauds with 5 times the amount
    for day in range(0, transactions.TX_TIME_DAYS.max(), 7):
        compromised = customers.CUSTOMER_ID.sample(n=1, random_state=day).values
        window = transactions[
            (transactions.TX_TIME_DAYS >= day)
            & (transactions.TX_TIME_DAYS < day + 14)
            & (transactions.CUSTOMER_ID.isin(compromised))
        ]
        random.seed(day)
        hit = random.sample(list(window.index.values), k=len(window) // 3)
        transactions.loc[hit, "TX_AMOUNT"] = transactions.loc[hit, "TX_AMOUNT"] * 5
        transactions.loc[hit, ["TX_FRAUD", "TX_FRAUD_SCENARIO"]] = [1, 3]

    return transactions


def main():
    customers = generate_customer_profiles(N_CUSTOMERS)
    terminals = generate_terminal_profiles(N_TERMINALS)
    x_y_terminals = terminals[["x_terminal_id", "y_terminal_id"]].values.astype(float)
    customers["available_terminals"] = customers.apply(
        lambda c: terminals_within_radius(c, x_y_terminals, RADIUS), axis=1
    )
    transactions = pd.concat(
        [generate_transactions(c, START_DATE, NB_DAYS) for _, c in customers.iterrows()]
    )
    transactions = transactions.sort_values("TX_DATETIME").reset_index(drop=True)
    transactions.reset_index(inplace=True)
    transactions.rename(columns={"index": "TRANSACTION_ID"}, inplace=True)
    transactions = add_frauds(customers, terminals, transactions)
    transactions["TX_AMOUNT"] = transactions["TX_AMOUNT"].round(2)
    transactions["TX_DATETIME"] = transactions["TX_DATETIME"].dt.strftime("%Y-%m-%d %H:%M:%S")
    transactions[
        [
            "TRANSACTION_ID",
            "TX_DATETIME",
            "CUSTOMER_ID",
            "TERMINAL_ID",
            "TX_AMOUNT",
            "TX_TIME_SECONDS",
            "TX_TIME_DAYS",
            "TX_FRAUD",
            "TX_FRAUD_SCENARIO",
        ]
    ].to_csv(OUTPUT, index=False, header=False, compression={"method": "gzip", "mtime": 0})
    print(
        f"{len(transactions)} transactions, {transactions.TX_FRAUD.sum()} frauds, "
        f"{transactions.CUSTOMER_ID.nunique()} customers, "
        f"{transactions.TERMINAL_ID.nunique()} terminals"
    )


if __name__ == "__main__":
    main()
