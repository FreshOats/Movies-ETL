# Movies: Extract, Transform, Load
**Extracting data from different sources, processing, and presenting the data via SQL**
#### by Justin R. Papreck
---

## Overview

This project extracts data from both Wikipedia and Kaggle to create clean data sets for a hack-a-thon for Amazing Prime. The wikipedia data was acquired in json form, while the Kaggle data was downloaded as a file with several csv files. The tables were inspected during exploratory data analysis, and then subsequently cleaned using Python. Following data cleaning, the tables were merged and uploaded to a PostgreSQL server for the hack-a-thon. 

--- 
## Exploratory Analysis and Cleaning of Wikipedia Data
### Cleaning Attributes

The Wikipedia data came in JSON form, and due to the nature of Wikipedia being open source with many people submitting information for movies, the data were very disorganized. After converting the json to a dataframe, the columns were investigated, only to find that multiple columns represented the same thing, such as 'Distributor' and 'Distributed by'. There were also a number of languages and language conversions that only seldom had movies associated with them. Since 'Language' is already a parameter, it is unnecessary to keep boolean copies of the individual languages as their own columns. Ultimmately 20 columns were removed for this reason. Additionally, 19 sets of similar entries with different names were grouped. This was put into the function clean_movie and applied throughout the analyses.

```
def clean_movie(movie):
    movie = dict(movie)  # Creates a non-destructive copy locally
    alt_titles = {}
    for key in ['Also known as', 'Arabic', 'Cantonese', 'Chinese', 'French',
                'Hangul', 'Hebrew', 'Hepburn', 'Japanese', 'Literally',
                'Mandarin', 'McCune–Reischauer', 'Original title', 'Polish',
                'Revised Romanization', 'Romanized', 'Russian',
                'Simplified', 'Traditional', 'Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)

    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name(... )
    etc.
    
    
    return movie
```

Since the purpose of this hack-a-thon is to work with movies that are included in the IMDB, it was necessary to remove the entries that were not in the IMDB, as well as remove entries with multiple episodes, since these would not be movies, but rather shows. Starting with the IMDB, each file has an IMDB link, but the IMDB number will be more useful, as the Kaggle Data only has the IMDB number, and no the whole URL. The IMDB number was extraced from the URL using regex, and a new column was created with the IMDB number - then the duplicate entries were removed. To further clean the data, columns were removed that contained over 90% null values.
    
```
wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum()
 < len(wiki_movies_df) * 0.9]
```

---
### Transforming the Monetary Data

The data types for each of the attributes were examined, and, huge surprise, most of them were not in proper forms for the data they represent. The biggest issues were the columns where money was involved, as there were two main forms in which the money was represented. The first form:  $ 4.5 million.  The second form $4,500,000.00.  There were additional formats as well, some given in ranges, and it would need to be determined how much of the data would be lost if ignoring everything not in one of the two main forms. 

Initially, the 'box office' information was dealt with. After dropping na records, a list comprehension with a lambda function was used to identify how many records are not in a string format:

```
box_office[box_office.map(lambda x: type(x) !=str)]
```

This returned 135 rows, but many of them appeared in list format that when joined would be in one of the two forms, so these lists were merged using a join function: 

```
box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
```

The function created to parse the dollar amounts using regex is as follows: 

```
def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) !=str:
        return np.nan


    # if input is of the form $###.# million

    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]', '', s)
        # convert to float and multiply by a million
        value = float(s) * 10**6
        # return value
        return value


    # if input is of the form $###.# billion

    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]', '', s)
        # convert to float and multiply by a billion
        value = float(s) * 10**9
        # return value
        return value


    # if input is of the form $###,###,###
    elif re.match( r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
        # remove dollar sign and commas
        s = re.sub('\$|,', '', s)
        # convert to float
        value = float(s)
        # return value
        return value


    # otherwise, return NaN
    else: 
        return np.nan
```


and when applied: 

```
wiki_movies_df['box_office'] = box_office.str.extract(
    f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
```

This same process was applied to the budget column as well. 


### Transforming the Date and Time Information

There were 4 types of dates that appeared in the data, and these needed to be converted to a consistent format. This was done similar to how the budget information was processed, though converting everything to a datetime format: 

```
release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]?\d,\s\d{4}'
date_form_two = r'\d{4}.[01]\d.[0123]\d'
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
date_form_four = r'\d{4}'

wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(
    f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
```

For the running time, the data were either in minutes or hours/minutes, so it was easier to use regex to transform this data.

```
running_time = wiki_movies_df['Running time'].dropna().apply(
    lambda x: ' '.join(x) if type(x) == list else x)
    
running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

running_time_extract = running_time_extract.apply(
    lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    
wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
```

---
## Explotory Analysis and Cleaning the Kaggle Data

Initially, the data types were explored, verifying where boolean values existed, that for all records they were boolean. There were 3 records that did not have boolean values in the 'adult' category, and upon further investiagation, none of these records had IMDB numbers, and the data didn't seem to match in any of the other columns. Furthermore, since this is a hack-a-thon for Amazing Prime, the scope of this doesn't include adult films, so all movies that did not yield a 'False' for adult was removed from the dataset. Similar to the Wikipedia processing, only movies that met the condition that it is a video were kept. From the Kaggle Metadata file, the budget, id, popularity, and release date were easily convertied to the appropriate data types, as shown: 

```
kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)

kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')

kaggle_metadata['popularity'] = pd.to_numeric(
    kaggle_metadata['popularity'], errors='raise')
    
kaggle_metadata['release_date'] = pd.to_datetime(
    kaggle_metadata['release_date'])
```

---
## Transforming the Data - Merging the Datasets
### Joining the Wikipedia and the Metadata

Joining the data was straghtforward: 

```
movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
```

Since many of the attributes are the same, it needed to be determined which to keep, and what to do with any null values. The columns that were compared are shown in the table below: 

![blank_table](https://user-images.githubusercontent.com/33167541/179100891-b7e46bbe-792c-4830-bdbd-bd6a910424ed.png)


Using scatter plots to show the values from both Wikipedia and Kaggle, it is shown where any discrepancies appear. Values that line up on the line y=x are in agreement from both sources. Those on the horizontal axis are null from Kaggle, and those on the vertical axis are null values from Wikipedia. One example of this is the budget comparison from the two sources: 

![budget_comparison](https://user-images.githubusercontent.com/33167541/179101193-ec390c7f-c44a-4f23-9dbe-4ac51ac3508f.png)

While it appears that kaggle has more null values than Wikipedia, the data from Wikipedia has many more outliers than does Kaggle (the data is farther right than up). For this reason, it was opted to use the Kaggle Data and fill in null values with the Wikipedia data. 

Ultimately, the following changes were to be made: 

![full_table](https://user-images.githubusercontent.com/33167541/179103225-fa414495-e010-4ba2-a019-82d8bbad7efe.png)

For the sections where Wikipedia columns were dropped: 
- Title: the title from Kaggle contained subtitles and were more informative
- The release dates from Kaggle were formatted the same way and contained far fewer Nan values
- The wikipedia contained differently formatted lists of language information and 134 Nan values, whereas Kaggle had no missing fields and no lists
- Production companies in wikipedia were all formatted differently, but in Kaggle they were entered in dictionaries


### Joining the Ratings Data

Prior to merging the ratings, the data needed to be pivoted and aggregated to be meaningful: 

```
rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId', columns='rating', values='count')
                
rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
```

Once these were found per movie, using the movie ID, the tables were merged, using a left merge to keep all data from the previously merged tables. To prevent a new batch of Nan values, the fields with missing rating information were filled with 0. 

```
movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on = 'kaggle_id', right_index=True, how='left')

movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
```


## Sending to the Database
Finally, with the datasets merged and cleaned, the two different data sets were sent to SQL: the movies dataframe with the merged data from wikipedia and kaggle, as well as the ratings dataframe, that has the individual user ratings. Because the datasets were so large, the import was done in chunks of 1 million records at a time. 

```
db_string = f"postgresql://postgres:{db_password}@127.0.0.1:5432/movie_data"
engine = create_engine(db_string)

rows_imported = 0
start_time = time.time()


for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):

    # print out the range of rows that are being imported
    print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    data.to_sql(name='ratings', con=engine, if_exists='append')
    rows_imported += len(data)
    
    
    print(f'Done. {time.time() - start_time} total seconds elapsed')
```

## The Functions

The first function, clean_movie() takes a single input for 'movie'. 
The second function, extract_transform_load() takes 3 inputs: 
- wiki-file
- kaggle_file
- ratings_file

the extract_transform_load() function first loads the three different files, then cleans the wikipedia data, cleans the kaggle metadata, merges those two tables, then processes the ratings data and merges that to the other merged tables. Where in the previous processing, the function returned the three dataframes, this function is designed to upload the data into the postgreSQL database, so using the three inputs, it creates two different tables in SLQ, one for the movies (the merged dataframes) and the transformed ratings table. 

To run the function, the code needed is as follows: 

```
file_dir = '../Resources'

wiki_file = f'{file_dir}/wikipedia-movies.json'
kaggle_file = f'{file_dir}/movies_metadata.csv'
ratings_file = f'{file_dir}/ratings.csv'

extract_transform_load(wiki_file, kaggle_file, ratings_file)
```

Upon examining the tables in SQL, the following record counts were found: 


![movies_query](https://user-images.githubusercontent.com/33167541/179116982-15a021c6-c452-4272-b1ad-de6696478ad4.png)


![ratings_query](https://user-images.githubusercontent.com/33167541/179116987-2dd356f7-253e-43ad-9021-e31aca8d9e0e.png)


Each of these yielded the expected number of records, so the provided tables to the database was successful, noting the completion of the project for Amazing Prime's hack-a-thon.
