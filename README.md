# MDLE First Assignment

The objective of this project was to implement the A-Priori algorithm to obtain the most frequent itemsets for a list of conditions for a large set of patients, obtaining then associations between conditions by extracting rules of the forms (X) -> Y and (X, Y ) -> Z. 
Another goal was to implement and apply LSH to identify similar news articles from a dataset.

## Course
This project was developed under the [Mining Large Scale Datasets](https://www.ua.pt/en/uc/14536) course of [University of Aveiro](https://www.ua.pt/).

## How to run

### Exercise 1

For each k (2 or 3), run the following command, inside the _/src/_ directory:

    spark-submit conditions.py <K> ../data/conditions.csv

For a sample run, execute:

    spark-submit conditions.py <K> ../data/conditions_truncated.csv

The results can be found inside the _/results/_ directory.

### Exercise 2

Run the following command, inside the _/src/_ directory:

    spark-submit lsh.py ../data/covid_news_truncated.json <R> <B>

## Grade 
This project's grade was **16,7** out of 20.

## Authors
* **Eduardo Santos**: [eduardosantoshf](https://github.com/eduardosantoshf)
* **Pedro Bastos**: [bastos-01](https://github.com/bastos-01)