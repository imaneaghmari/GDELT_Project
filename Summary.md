# Presentation of the database GDELT

Supported by Google Jigsaw, the GDELT Project monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages and identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day, creating a free open platform for computing on the entire world.

https://www.gdeltproject.org/

## EVENT Table

The GDELT Event Database records over 300 categories of physical activities around the world, from riots and protests to peace appeals and diplomatic exchanges, georeferenced to the city or mountaintop, across the entire planet dating back to January 1, 1979 and updated every 15 minutes.

Essentially it takes a sentence like "The United States criticized Russia yesterday for deploying its troops in Crimea, in which a recent clash with its soldiers left 10 civilians injured" and transforms this blurb of unstructured text into three structured database entries, recording US CRITICIZES RUSSIA, RUSSIA TROOP-DEPLOY UKRAINE (CRIMEA), and RUSSIA MATERIAL-CONFLICT CIVILIANS (CRIMEA).

Nearly 60 attributes are captured for each event, including the approximate location of the action and those involved. This translates the textual descriptions of world events captured in the news media into codified entries in a grand "global spreadsheet."

http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf


Event Geography:

 * The Action fields capture the location information closest to the point in the event description that contains the actual statement of actionand is the best location to use for placing events on a map or in other spatial context

 * To  find  all  events  located  in  or  relating  to  a  specific  city  or  geographic  landmark,  the  Geo_FeatureID column  should  be  used,  rather  than  the  Geo_Fullname  column. 

 * When looking for events in or relating to a specific country, such as Syria, there are two possible filtering methods.    The  first  is  to  use  the  Actor_CountryCode  fields  in  the  Actor  section  to  look  for  all  actors havingthe  SYR  (Syria)  code
 * The second method is to examine the  ActorGeo_CountryCode  for  the  location  of  the  event.    This  will  also  capture  situations  such  as  the United States criticizing a statement by Russia regarding a specific Syrian attack


## Mention Event Table 

The Mentions table is a new addition to GDELT 2.0 and records each mention of the events in the Event table, making it possible to track the trajectory and network structure of a story as it flows through the global media system.  Each mention of an event receives its own entry in the Mentions table –thus an event  which  is  mentioned  in  100  articles  will  be  listed  100  times  in  the  Mentions  table.    Mentions  are recorded irrespective of the date of the original event, meaning that a mention today of an event from a year ago will still be recorded, making it possible to trace discussion of “anniversary events” or historical events  being  recontextualized  into  present  actions.    If  a  news  report  mentions  multiple  events,  each mention  is  recorded  separately  in  thistable.For  translated  documents,  all measures  below  are  based on its English translation



## GKG (Global Knowledge Graph) table

http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf

V2.1TRANSLATIONINFO.    (semicolon-delimited  fields)  This  field  is  used  to  record  provenance information  for  machine  translated  documents  indicating  the  original  source  language  and  the citation of the translation system used to translate the documentfor processing.  It will be blank for  documents  originally  in  English.


# Fonctionnalités Nécessaire

## Fonctionalités de base 

Votre système doit être capable de:
 * afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).
    
GKG table:

>  Group By sur:
>   - DATE
>   - V2Locations 
>   - TRANSLATIONINFO : langue de l'article<br>
> Puis un count 

 * pour un pays donné en paramètre, affichez les évènements qui y ont eu place triées par le nombre de mentions (tri décroissant); permettez une agrégation par jour/mois/année
Event Table:

> SELECT GLOBALEVENTID <br>
> WHERE Pays = Actor1Geo_ADM2Code <br>
> GROUP BY: DATE<br>
> ORDER BY: NumMentions DESC<br>
 

 * pour une source de donnés passée en paramètre (gkg.SourceCommonName) affichez les thèmes, personnes, lieux dont les articles de cette sources parlent ainsi que le le nombre d’articles et le ton moyen des articles (pour chaque thème/personne/lieu); permettez une agrégation par jour/mois/année.

GKG table:

> SELECT: <br>
> V2Themes, V2Persons, V2Locations , V2Tone, Count<br>
> WHERE Source donné == SourceCommonName<br>
> GROUP BY Date<br>

 * dresser la cartographie des relations entre les pays d’après le ton des articles : pour chaque paire (pays1, pays2), calculer le nombre d’article, le ton moyen (aggrégations sur Année/Mois/Jour, filtrage par pays ou carré de coordonnées)

GKG table:

> SELECT TranslationInfo, V2Locations , V2Tone, Count <br>
> GROUP BY Date


## Reduction de la Base de donnée pour notre étude

<img src="./Diagram/dbschema.png"></img>

Les éléments de la table *event* et *eventmention* semblent être suffisant pour le cadre de notre étude.

# Choix de la Base de donnée

Nous avons plusieurs possibilité pour le choix de la base de données:

 * PostGreSQL
 * Neo4j
 * Cassandra
 * MongoDB

Pour la modélisation de cette base, nos contraintes sont les suivantes:

    Vous devez utiliser au moins 1 technologie vue en cours en expliquant les raisons de votre choix (SQL/Cassandra/MongoDB/Spark/Neo4j).

    Vous devez concevoir un système distribué et tolérant aux pannes (le système doit pouvoir continuer après la perte d’un noeud).

    Vous devez pre-charger une année de données dans votre cluster

    Vous devez utiliser AWS pour déployer le cluster.

En raison de la grande quantitée de données à traiter, et de la nécessité d'avoir un système résiliant à la panne, il nous faut élaborer un système distribué.

En raison des différentes fonctionnalités auquel il nous faut répondre, nous avons besoin d'un système effectuant des requêtes d'aggregation de manière performante.

## SQL

Même si SQL a beaucoup d'avantage avec ses requêtes ACID, ce n'est pas un système qui est facile à déployer sur un cluster de machine. Aujourd'hui il existe des base de donnée appelée 'New SQL' essayant de relevé c'est problématique. Mais comme nous ne connaissons pas bien ces technologies, nous ne les aborderons pas dans notre étude.

## Neo4j

Neo4j est une base de donnée orienté graph elle permet donc d'avoir une meilleur visualisation de nos données. Néanmoins seul la version entreprise permet un deployement sur un cluster. On pourra si nous avons le temps nécessaire analyser les schéma de donnée graph sur une base Neo4j monté sur un seul noeud, mais ce ne sera la priorité du projet.

## Cassandra

Cassandra est une base de donnée distribué. Là où les base de données SQl sont dites ACID, Cassandra est dites BASE pour **B**asically **A**vailable, **S**oft state, **E**ventually consistent. Les systèmes BASE privilégie la disponibilité et la tolérance du système au détriment de la cohérence. 

    Basically available : garantie la disponibilité
    Soft state: L'etat du sytème peut être modifié au cours du temps même sans input du au mise à jour pour atteindre la cohérence du système par exemple.
    Eventual consistency: Si le système ne reçoit plus de requête d'insertion il finira par être cohérent.

Cassandra semble donc répondre à nos critère. Néanmoins un détail peut être problématique, Cassandra n'est pas optimiser pour scanner plusieurs un grand nombre de ligne. 

Etant donnée que nos fonctionnalité nous demande des fonction aggregation tel que des groupement ou des compte, il sera préférable de faire c'est traitement en amont via spark par exemple et de stocjer les résultats dans des tables Cassandra.

## MongoDB

MongoDB est une base de donnée orienté document, elle est très perfomante mais n'a pas non plus de possibilité d'aggregation de calcul. Son avantage est que le schema des donnée peut être différent dans les différents documents enregistré en base. Comme nous voulons stocker des données ayant subi un préprocessing via spark, cette fonctionalité ne nous intéresse pas.


## Conclusion

Au vu de nos différent choix Cassandra semble le plus optimisé pour la taille de notre projet. De plus comme Cassandra est Basé sur la JVM comme Scala, il sera plus simple de faire la connection entre les deux.

# Modélisation de la base de donnée

## EM Diagram

> Utilisiation de https://www.draw.io/

<img src="./Diagram/EM_diagram.png"></img>

## Logical Model

> Utilisation de https://app.dbdesigner.net

Pour décrire notre modèle logique, nous allons nous baser sur les querry au quel nous devons répondre.

1.  Afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).
(Table GKG)

Partition Key -> Date
Clustering Key -> Pays de l'evenement, langue de l'article
-> Counter column 

<center>
<img src="./Diagram/Logique/counter_article_by_date.png"></img>
</center>

2. Pour un pays donné en paramètre, affichez les évènements qui y ont eu place, triées par le nombre de mentions (tri décroissant); permettez une agrégation par jour/mois/année (Table Event)

Partition key ~ 

Partition Key -> Pays 
Clustering Key ->Nombre de mention DESC

Aggregation par Date à voir Secondary Index ?

<center>
<img src="./Diagram/Logique/events_by_location.png"></img>
</center>

3. Pour une source de donnés passée en paramètre (gkg.SourceCommonName) affichez les thèmes, personnes, lieux dont les articles de cette sources parlent ainsi que le le nombre d’articles et le ton moyen des articles (pour chaque thème/personne/lieu); permettez une agrégation par jour/mois/année. (Table GKG)


 !!! Partition key fausse
2 Tables 
Table Counter
Partition Key-> Source Common Name
Clustering Key -> thèmes, personnes, lieux
Counter
Sum Tone

Table Querry
Partition Key-> Source Common Name   
Clustering Key -> thèmes, personnes, lieux
Counter querry sur la table 
Tone moyen Querry sur la table

<center>
<img src="./Diagram/Logique/querry3.png"></img>
</center>

4. Dresser la cartographie des relations entre les pays d’après le ton des articles : pour chaque paire (pays1, pays2), calculer le nombre d’article, le ton moyen (aggrégations sur Année/Mois/Jour, filtrage par pays ou carré de coordonnées) (Table GKG)

Partition Key -> ?
Clustering KEy -> Pays1, pays2