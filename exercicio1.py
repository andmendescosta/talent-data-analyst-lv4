import pandas as pd
from itertools import cycle

def cpf_validate(numbers):
    #  Obtém os números do CPF e ignora outros caracteres
    cpf = [int(char) for char in numbers if char.isdigit()]

    #  Verifica se o CPF tem 11 dígitos
    if len(cpf) != 11:
        return False

    #  Verifica se o CPF tem todos os números iguais, ex: 111.111.111-11
    #  Esses CPFs são considerados inválidos mas passam na validação dos dígitos
    #  Antigo código para referência: if all(cpf[i] == cpf[i+1] for i in range (0, len(cpf)-1))
    if cpf == cpf[::-1]:
        return False

    #  Valida os dois dígitos verificadores
    for i in range(9, 11):
        value = sum((cpf[num] * ((i+1) - num) for num in range(0, i)))
        digit = ((value * 10) % 11) % 10
        if digit != cpf[i]:
            return False
    return True

def cnpj_validate(cnpj: str) -> bool:
    if len(cnpj) != 14:
        return False

    if cnpj in (c * 14 for c in "1234567890"):
        return False

    cnpj_r = cnpj[::-1]
    for i in range(2, 0, -1):
        cnpj_enum = zip(cycle(range(2, 10)), cnpj_r[i:])
        dv = sum(map(lambda x: int(x[1]) * x[0], cnpj_enum)) * 10 % 11
        if cnpj_r[i - 1:i] != str(dv % 10):
            return False

    return True

def normalize_estado(estado):
    if len(estado) != 2:
        return 'N/A'
    else:
        return estado

df = pd.read_csv ('./dados_cadastrais_fake.csv', sep=';')
df['cpf'] = df['cpf'].str.replace('\W', '', regex=True)
df['cnpj'] = df['cnpj'].str.replace('\W', '', regex=True)
df['cpf_valido'] = df['cpf'].apply(lambda x: cpf_validate(x))
df['cnpj_valido'] = df['cnpj'].apply(lambda x: cnpj_validate(x))
df['estado'] = df['estado'].apply(lambda x: normalize_estado(x))
print(f"Quantos clientes temos nessa base: {df['nomes'].count()}")
print(f"Qual a média de idade dos clientes: {df['idade'].mean()}")

df_grouppedby_estado = df.groupby(['estado'])['estado'].count()
print("Quantos clientes nessa base pertencem a cada estado:")
print(df_grouppedby_estado)

print("Quantos CPFs válidos e inválidos foram encontrados:")
print(f"CPFs Válidos:{df.loc[df['cpf_valido'] == True]['nomes'].count()}")
print(f"CPFs Inválidos:{df.loc[df['cpf_valido'] == False]['nomes'].count()}")
print(f"CNPJs Válidos:{df.loc[df['cnpj_valido'] == True]['nomes'].count()}")
print(f"CNPJs Inválidos:{df.loc[df['cnpj_valido'] == False]['nomes'].count()}")

df.to_csv('./output/exercicio1.csv', sep=';', encoding='utf-8')
df.to_parquet('./output/exercicio1.parquet', engine='fastparquet')