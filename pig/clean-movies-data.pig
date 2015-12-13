rmf /user/eyalb/movies-fixed
raw = LOAD '/user/eyalb/ml-latest-small/movies.csv' USING TextLoader() AS (line:chararray);
parsed = FOREACH raw {
    first_index = INDEXOF(line, ',');
    last_index = LAST_INDEX_OF(line, ',');

    movieId = (int)SUBSTRING(line, 0, first_index);
    full_title = REPLACE(REPLACE(SUBSTRING(line, first_index + 1, last_index), '"', ''), ',', '');
    length = (int)SIZE(line);
    genres = SUBSTRING(line, last_index + 1, length);

    full_title_length = (int)SIZE(full_title);
    year = (int)TRIM(
                REPLACE(
                    REPLACE(
                        SUBSTRING(full_title, full_title_length - 7, full_title_length)
                    ,'\\(', '')
                ,'\\)', '')
            );
    title = REPLACE(SUBSTRING(full_title, 0, full_title_length - 7), '"', '');

    GENERATE movieId,title,year,genres;
};

filtered = FILTER parsed BY $0 > 0;
STORE filtered INTO '/user/eyalb/movies-fixed';