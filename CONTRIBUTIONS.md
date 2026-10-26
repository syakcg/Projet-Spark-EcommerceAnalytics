#  Journal des Contributions & Historique du Projet

Nous retraçons  le journal des contributions individuelles et collectives réalisées lors du développement du projet **EcommerceAnalytics**.

## Tableau Récapitulatif des Tâches

| Question / Module du Projet | Membre Responsable | Membre Relecteur |
| --- | --- | --- |
| **Partie 1** (Initialisation de l'environnement, SBT et structure du projet) | Membre A | Membre C |
| **Partie 2** ( INGESTION ET VALIDATION DES DONNÉES) | Membre A | Membre B |

---
---

## Partie 1 : Infrastructure, Configuration & Modélisation (Membre A)
* **Configuration du projet SBT** : Création de `build.sbt` avec déclaration des dépendances Spark 3.4.1, Typesafe Config et ScalaTest.
* **Externalisation de la configuration** : Rédaction de `application.conf` (HOCON) pour centraliser les chemins d'accès, paramètres Spark (`master`, `shuffle.partitions`) et seuils métier.
* **Développement du chargeur de configuration** : Création du singleton `ConfigLoader.scala` avec résolution des conflits d'importation de packages (`_root_.com.typesafe`).
* **Modélisation typée** : Définition des `case class` Scala dans `Models.scala` pour garantir le typage fort sur les `Dataset[T]`.

---
## Partie 2 : Ingestion Multi-format & Contrôle Qualité (Membre A)
* **Ingestion sécurisée (`DataIngestion.scala`)** :
    * Implémentation du chargement explicite CSV pour `transactions.csv`.
    * Ingestion JSON pour `users.json`.
    * Ingestion Parquet optimisée pour `products.parquet`.
    * Encapsulation des lectures dans des blocs `Try/Catch` pour la tolérance aux pannes.
* **Validation & Gestion des rejets (`DataValidation.scala`)** :
    * Application des règles de validation métier sur le montant, l'âge, les notes et taux de commission.
    * Tagging automatique des enregistrements invalides avec `withColumn("rejection_reason", ...)`.
    * Implémentation de la méthode `generateQualityReport` calculant le taux de rejet (%) et le comptage global des valeurs nulles.

---
