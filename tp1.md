---
title: Outils pour l'analyse des donnnées massives - TP1
author: Alexandre COMBESSIE, Gabriel DUCROCQ
---


### Exercice 1
Tout a bien fonctionné pour se connecter au cluster AWS sur Windows 8 et utiliser Spark-shell/Scala.

![Capture d'écran](https://dl.dropbox.com/s/45v3jmq7whqr0kd/image%201.png?dl=0)

**********
### Exercice 2

1) La dernière ligne revient à calculer la variance empirique :
$\frac{1}{n} \sum_{i=1}^n x_i^2-(\frac{1}{n} \sum_{i=1}^n x_i)^2$
On note l'utilisation de `map` pour appliquer la fonction à chaque élément de la liste, et celle de `reduce` avec l'opérateur `_+_` pour additionner tous les éléments de la liste.
```Spark
val lf = List(3.2,1.1,0.9,2.3,2.5,1.7
lf.map{x=>x*x}.reduce(_+_)/lf.length - Math.pow(lf.reduce(_+_)/lf.length,2)
```

2) La seule différence par rapport à la question précédente est l'utilisation de `sc.parallelize` pour distributer le stockage de la liste sur la RAM des différentes machines du cluster. Le traitement des `map` et `reduce` sera lui aussi distribué sur les machines concernées. Dans le cas présent, la liste est très petite donc la parallélisation n'est pas nécessaire. Cependant cela va vite le devenir avec la question 3.
```Spark
val lf = sc.parallelize(List(3.2,1.1,0.9,2.3,2.5,1.7))
lf.map{x=>x*x}.reduce(_+_)/lf.length - Math.pow(lf.reduce(_+_)/lf.length,2)
```

3) Ce script permet de calculer approximativement l'aire d'un quart de cercle unitaire (soit $\frac{\pi}{4}$) selon une approche de Monte Carlo avec 1 million de tirages.
```Spark
/** Version parallélisée */
val NUM_SAMPLES = 1000000
val count = sc.parallelize(1  to NUM_SAMPLES).map{i =>
  val x = Math.random()
  val y = Math.random()
  if (x*x+y*y<1) 1 else 0
}.reduce(_+_)
println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)
```

4) Le script tourne en 0.95 secondes en version parallélisée, contre 0.10 secondes en version non parallélisée. La parallélisation ne permet donc pas de réduire le temps de calcul, ce qui peut s'expliquer par la petite taille des données et le temps de parallélisation de l'étape `reduce`. Il faudrait comparer les deux scripts pour un nombre de tirages plus élevé : 100 millions. Cependant, cela ne change pas le constat : la version parallélisée prend 54 secondes, contre 9 secondes pour la non parallélisée. Tant que les donnés rentrent en mémoire vive, la parallélisation n'apporte pas d'amélioration. Le problème peut également venir des temps d'échange de données sur le réseau entre les noeuds du cluster.
```Spark
/** Version non parallélisée */
val NUM_SAMPLES = 1000000
val count = (1  to NUM_SAMPLES).map{i =>
  val x = Math.random()
  val y = Math.random()
  if (x*x+y*y<1) 1 else 0
}.reduce(_+_)
println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)
```

5) Cette fonction récursive correspond à l'algorithme de tri classique de *Merge Sort*. On divise la liste à trier en deux sous-parties, que l'on trie en réappliquant la même logique jusqu'à arriver à une paire de valeurs (facile à ordonner). En testant cette fonction sur une petite liste de test, on voit que cet algorithme fonctionne bien.
```Spark
def mystere(xs: Array[Int]): Array[Int] ={
  if (xs.length<=1) xs
  else {
    val p = xs(xs.length/2)
    Array.concat(
      mystere(xs filter (p >)),
      xs filter (p ==),
      mystere(xs filter (p <)))
      )
  }
}
val a = Array(5,3, 2, 2, 1, 1, 9, 39, 219)
mystere(a)
```

********

### Exercice 3

1) Grâce à la méthode `count` on voit facilement qu'il y a 317 053 pages dans le dataset.
```Spark
val enwiki = sc.textFile("/enwiki")
enwiki.count()
```

2) On accède aux dix premières pages avec la méthode `take(10)`. On voit le début d'articles sur les sujets suivants : Magnetism, Hypnosis, Italian Writers, Mineralogy, Fur Trade, Scientific Techniques, Race and Intelligence Controversy, Forms of water, Aquatic Ecology, Cardinal numbers.
```Spark
val first_ten = enwiki.take(10)
```



3) On forme facilement des tuples sur la catégorie et le topic de l'article en extrayant les deux premiers mots de chaque article avec `split`. Pour les dix premiers éléments, on obtient ainsi une ((NATURE,MAGNETISM), (LIFE,HYPNOSIS), (ARTS,ITALIAN_WRITERS), (NATURE,MINERALOGY), (SOCIETY,FUR_TRADE), (HEALTH,SCIENT   IFIC_TECHNIQUES), (LIFE,RACE_AND_INTELLIGENCE_CONTROVERSY), (BUSINESS,FORMS_OF_WATER), (BUSINESS,AQUATIC_ECOLOGY), (LANGUAGE,CARDINAL_NUMBERS)).
```Spark
val tuple_cat_topic = enwiki.map{i=> (i.split("\t")(0),i.split("\t")(1))}
tuple_cat_topic.take(10)
```

4) On utilise la méthode `reduceByKey(_+_)`pour faire la somme des articles par catégorie. Cela forme un tuple $(key: categorie, value: somme)$ qu'on peut trier avec `sortBy` sur son deuxième élément. Par ordre croissant, on obtient ainsi : (SCIENCE,632), (CHRONOLOGY,799), (HISTORY,962), (SPORTS,1333), (LAW,1361), (PEOPLE,1441), (HEALTH,1890), (MEDICINE,1979), (MATHEMATICS,2542), (EDUCATION,3225), (AGRICULTURE,3250), (BELIEF,3802), (GEOGRAPHY,4394), (TECHNOLOGY,4942), (POLITICS,6806), (CULTURE,8350), (BUSINESS,10297), (HUMANITIES,13442), (ENVIRONMENT,16479), (NATURE,21461), (ARTS,42258), (LANGUAGE,44591), (SOCIETY,50459), (LIFE,70358)).
```Spark
val sum_by_category = enwiki.map{i=> (i.split("\t")(0),1)}.reduceByKey(_+_)
sum_by_category.collect()
sum_by_category.sortBy(_._2)
```



5) Pour calculer le nombre de topics qui font l'objet de plus de 1000 articles, il suffit d'appliquer la même logique que précédemment. On forme un tuple $(key: topic, value: somme)$  que l'on filtre avec la méthode `filter`. On obtient 28 303 articles comme résultat.
```Spark
val sum_by_topic = enwiki.map{i=> (i.split("\t")(1),1)}.reduceByKey(_+_)
sum_by_topic.collect()
val sum_by_topic_filtered = sum_by_topic.filter(e => e._2 > 1000)
sum_by_topic_filtered.collect()
sum_by_topic.count()
```



6) L'idée est de former un double tuple $\Big(key: (categorie,topic),value: somme \Big)$ avec `reduceByKey(_+_)`. Ensuite, on fait un `reduceByKey` sur cette variable en extrayant la catégorie de la clé-tuple. Cela permet de constituer une `Array` contenant le nombre de catégories par topic. Il suffit alors de filtrer sur les valeurs supérieures à 1.
```Spark
val tuple_cat_topic_sum = enwiki.map{
  i=> ((i.split("\t")(0),i.split("\t")(1)), 1)
  }.reduceByKey(_+_)
val tuple_cat_topic_sum2 = tuple_cat_topic_sum.reduceByKey(_._1)
val tuple_cat_topic_sum3 = tuple_cat_topic_sum2.filter(e => e._2 > 1)
```
