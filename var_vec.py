import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller, coint, grangercausalitytests
from statsmodels.tsa.vector_ar.vecm import VECM
from statsmodels.tsa.vector_ar.var_model import VAR


def check_stationarity(series, alpha=0.05):
    # Realiza o teste de raiz unitária (Augmented Dickey-Fuller)
    result = adfuller(series)
    p_value = result[1]
    # se o p_value for menor que alpha a serie é estacionaria
    return p_value < alpha

def adjust_series(series):
    # Aplica a primeira diferença
    return series.dropna().diff().dropna()

def make_stationarity(df):
    # Ajusta a serie pela diferença até se tornar estacionária
    stationarity_y = False
    stationarity_x = False
    adj_series_y = df['y']
    adj_series_x = df['x']
    diff = 0

    while not (stationarity_y & stationarity_x):
      adj_series_y = adjust_series(adj_series_y)
      stationarity_y = check_stationarity(adj_series_y)

      adj_series_x = adjust_series(adj_series_x)
      stationarity_x = check_stationarity(adj_series_x)

      diff += 1

    df['y'] = adj_series_y
    df['x'] = adj_series_x

    return df, diff

def execute_model(df, alpha=0.05):
    # Verifica se as séries são estacionárias
    is_y_stationary = check_stationarity(df['y'], alpha)
    is_x_stationary = check_stationarity(df['x'], alpha)
    
    # Determina o tipo de modelo a ser utilizado
    if is_y_stationary and is_x_stationary:
        model_type = 'VAR'
    else:
        score, pvalue, _ = coint(df.y, df.x, maxlag = 1)
        model_type = 'VEC' if pvalue < alpha else 'VAR_DIFF'
    
    # Executa o modelo VAR ou VEC
    if model_type == 'VAR':
        model = VAR(df[['y', 'x']])
        results = model.fit()
        causal_relationship = results.test_causality('y', 'x', kind='f')
        p_value_causal = causal_relationship.pvalue
    elif model_type == 'VEC':
        model = VECM(df[['y', 'x']])
        results = model.fit()
        granger_test_result = grangercausalitytests(df[['y', 'x']], maxlag=1, verbose=False)
        p_value_causal = granger_test_result[1][0]['ssr_ftest'][1]
    else:
        df, diff = make_stationarity(df)
        df = df.dropna()
        model_type = f'VAR_DIFF_{diff}'
        model = VAR(df[['y', 'x']])
        results = model.fit()
        causal_relationship = results.test_causality('y', 'x', kind='f')
        p_value_causal = causal_relationship.pvalue

    return model_type, p_value_causal

# Exemplo de uso:
# Supondo que o dataframe seja chamado de df
# model_type, p_value_causal = execute_model(df, alpha=0.05)
# https://rpubs.com/hudsonchavs/vec#:~:text=Se%20eles%20s%C3%A3o%20n%C3%A3o%20estacion%C3%A1rios,de%20Corre%C3%A7%C3%A3o%20de%20Erros%20(VEC)
