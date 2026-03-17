import kagglehub
import os
import shutil

# 1. Définir le chemin de destination
# On utilise un chemin relatif ou absolu vers dags/data
target_dir = os.path.join(os.getcwd(), "dags/data")

# Créer le dossier s'il n'existe pas encore
os.makedirs(target_dir, exist_ok=True)

# 2. Télécharger le dataset (dans le cache temporaire)
temp_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")

# 3. Déplacer les fichiers vers dags/data
# Note : On boucle sur les fichiers pour éviter d'écraser le dossier 'data' lui-même
for filename in os.listdir(temp_path):
    source_file = os.path.join(temp_path, filename)
    destination_file = os.path.join(target_dir, filename)
    
    # Déplacement (shutil.move gère aussi le remplacement si nécessaire)
    shutil.move(source_file, destination_file)

print(f"Les données ont été déplacées avec succès vers : {target_dir}")