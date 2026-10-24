#  Journal des Contributions et Historique du Projet

Nous retraçons  le journal des contributions individuelles et collectives réalisées lors du développement du projet **EcommerceAnalytics**.

---

## Partie 1 : Infrastructure, Configuration et Modélisation (Membre A)
* **Configuration du projet SBT** : Création de `build.sbt` avec déclaration des dépendances Spark 3.4.1, Typesafe Config et ScalaTest.
* **Externalisation de la configuration** : Rédaction de `application.conf` (HOCON) pour centraliser les chemins d'accès, paramètres Spark (`master`, `shuffle.partitions`) et seuils métier.
* **Développement du chargeur de configuration** : Création du singleton `ConfigLoader.scala` avec résolution des conflits d'importation de packages (`_root_.com.typesafe`).
* **Modélisation typée** : Définition des `case class` Scala dans `Models.scala` pour garantir le typage fort sur les `Dataset[T]`.

---
## Partie 2 : Ingestion Multi-format et Contrôle Qualité (Membre A)
* **Ingestion sécurisée (`DataIngestion.scala`)** :
    * Implémentation du chargement explicite CSV pour `transactions.csv`.
    * Ingestion JSON pour `users.json`.
    * Ingestion Parquet optimisée pour `products.parquet`.
    * Encapsulation des lectures dans des blocs `Try/Catch` pour la tolérance aux pannes.
* **Validation et Gestion des rejets (`DataValidation.scala`)** :
    * Application des règles de validation métier sur le montant, l'âge, les notes et taux de commission.
    * Tagging automatique des enregistrements invalides avec `withColumn("rejection_reason", ...)`.
    * Implémentation de la méthode `generateQualityReport` calculant le taux de rejet (%) et le comptage global des valeurs nulles.


## 1. Tableau Récapitulatif des Tâches

| Question / Module du Projet | Membre Responsable | Membre Relecteur |
| --- | --- | --- |
| **Partie 4.1 et 4.2** (Rapports marchands et Analyse de cohortes utilisateurs) | Membre C | Membre B |
| **Partie 4.3 - (Bonus)** (Segmentation RFM ) | Membre C | Membre B |
| **Partie 5.1 et 5.2** (Optimisations : Cache, Persist, Broadcast, Jointures) | Membre C | Membre A et Membre B|
| **Partie 6.1 et [6.2 (Bonus)]** (Application principale `MainApp` et Exécution modulaire) | Membre C | Validé par tout le groupe |

---

## 2. Bilan Individuel — Membre C

* **Charge de travail estimée :** ~14 heures
* **Modules pris en charge :** Rapports marchands et Analyse de cohortes utilisateurs (4.1 et 4.2), segmentation RFM (4.3), Optimisations de stockage et de jointures Spark (5.1 et 5.2), Orchestration du pipeline principal et mode modulaire par arguments CLI (`MainApp.scala` - 6.1 et 6.2).
* **Difficultés rencontrées :**
* Incompatibilité de l'API DataFrame lors de l'appel direct de la méthode de tableau croisé *(Solution : Adaptation de la syntaxe en utilisant `.stat.crosstab`)*.
* Risque de saturation de la mémoire vive (*Out of Memory*) lors du traitement et de la jointure de volumes de données volumineux *(Solution : Configuration fine des niveaux de persistance sérialisée avec `MEMORY_AND_DISK_SER`)*.
* Risque de plantage de l'application ou d'exceptions non gérées en cas de saisie d'un paramètre incorrect par un utilisateur *(Solution : Gestion rigoureuse du parsing des arguments en ligne de commande et mise en place d'un guide d'aide textuel dans `MainApp.scala`)*.

---

## 3. Décisions Techniques du Groupe

1. **Choix des versions (Scala 2.12.18 et Apache Spark 3.4.1 sous Java 17) :**
* *Justification :* Ce stack technologique garantit une parfaite stabilité de compilation et de compatibilité avec les bibliothèques Big Data actuelles. L'utilisation de Java 17 avec les options de module appropriées (`--add-opens`) évite les erreurs de réflexion mémoire sous Windows.


2. **Stratégie de jointure avec `broadcast()` :**
* *Justification :* L'utilisation systématique de la fonction `broadcast()` sur la table des marchands (`dfMerchants`) permet d'éliminer le coût réseau du *shuffle* en dupliquant la petite table de dimension sur l'ensemble des exécuteurs, accélérant ainsi significativement les performances.


3. **Gestion intelligente du stockage (`cache()` et `persist`) :**
* *Justification :* L'application de `.cache()` sur le DataFrame des transactions réutilisé évite des relectures disques redondantes. Pour les tables issues de jointures lourdes, l'utilisation de `persist(StorageLevel.MEMORY_AND_DISK_SER)` compresse les objets en mémoire et bascule sur le disque en cas de dépassement de RAM.


4. **Double format de sortie des résultats (CSV et Parquet) :**
* *Justification :* L'export au format CSV a été retenu pour faciliter la lecture directe et le contrôle humain des résultats, tandis que le format Parquet a été choisi pour sa compression colonnaire hautement performante, standard de l'écosystème Big Data.


5. **Architecture modulaire de l'application (`MainApp.scala`) :**
* *Justification :* L'implémentation d'un système d'arguments en ligne de commande (`ingestion`, `transformation`, `analytics`, `all`) permet à chaque membre du groupe de tester isolément sa brique fonctionnelle sans avoir à exécuter l'intégralité du pipeline à chaque itération.

---

## Bilan de Relecture — Rôle de Relecteur - Membre C (Partie 1 / Membre A)

* **Module relu :** Partie 1 (Initialisation de l'environnement, structure du projet et configuration SBT).
* **Date de l'échange après relecture :** 23 octobre 2026 à 17h00.
* **Format de l'interaction :** Appel de synchronisation d'une dizaine de minutes pour auditer et valider l'ensemble des livrables de la Partie 1.
* **Difficultés rencontrées lors de la relecture :**
* Impossibilité initiale d'exécuter et de tester le code en local *(Solution : Identification et correction collective d'une configuration inadaptée dans le fichier `build.sbt` qui bloquait les tests locaux, permettant ainsi de rétablir une compilation stable)*.

---
