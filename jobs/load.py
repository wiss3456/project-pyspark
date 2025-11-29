def load_to_silver(df, output_path: str):
    # Écriture en CSV pour tester sur Windows
    df.write.mode("overwrite").csv(output_path, header=True)

# `df.write` : prépare l’écriture
# `.mode("overwrite")` : si le dossier existe déjà, il sera écrasé
# `.csv(output_path, header=True)` : écrit en CSV avec les noms de colonnes
