from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MovieAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_init),
            MRStep(reducer=self.reducer_final)
        ]

    def mapper(self, _, line):
        fields = line.split(',')
        if len(fields) == 5 :
            
            try:
                user, movie, rating, genre, date = fields
                rating = int(rating)
                yield ('user_count', user), (movie, 1)
                yield ('user_rating', user), (rating, 1)
                yield ('movie_count', movie), (user, 1)
                yield ('movie_rating', movie), (rating, 1)
                yield ('date_count', date), (movie, 1)
                yield ('date_rating', date), (rating, 1)
                yield ('genre_rating', (genre, movie)), rating
            except:
                pass


    def reducer_init(self, key, values):
        if key[0] == 'user_count':
            yield 'user_movies', (key[1], len(set(movie for movie, count in values)))
        elif key[0] == 'user_rating':
            total_rating, count = 0, 0
            for rating, cnt in values:
                total_rating += rating
                count += cnt
            yield 'user_avg_rating', (key[1], total_rating / count)
        elif key[0] == 'movie_count':
            yield 'movie_users', (key[1], len(set(user for user, count in values)))
        elif key[0] == 'movie_rating':
            total_rating, count = 0, 0
            for rating, cnt in values:
                total_rating += rating
                count += cnt
            yield 'movie_avg_rating', (key[1], total_rating / count)
        elif key[0] == 'date_count':
            yield 'date_movies', (key[1], sum(count for movie, count in values))
        elif key[0] == 'date_rating':
            total_rating, count = 0, 0
            for rating, cnt in values:
                total_rating += rating
                count += cnt
            yield 'date_avg_rating', (key[1], total_rating / count)
        elif key[0] == 'genre_rating':
            genre_movie = key[1]
            ratings = list(values)
            yield 'genre_movie_rating', (genre_movie, sum(ratings) / len(ratings))

    def reducer_final(self, key, values):
        if key == 'user_movies':
            for user, movie_count in values:
                yield f"User {user} has seen {movie_count} movies.", None
        elif key == 'user_avg_rating':
            for user, avg_rating in values:
                yield f"User {user} has an average rating of {avg_rating:.2f}.", None
        elif key == 'movie_users':
            for movie, user_count in values:
                yield f"Movie {movie} has been seen by {user_count} users.", None
        elif key == 'movie_avg_rating':
            for movie, avg_rating in values:
                yield f"Movie {movie} has an average rating of {avg_rating:.2f}.", None
        elif key == 'date_movies':
            date_views = list(values)
            most_viewed_date = max(date_views, key=lambda x: x[1])
            least_viewed_date = min(date_views, key=lambda x: x[1])
            yield f"The day with the most movies watched is {most_viewed_date[0]}.", None
            yield f"The day with the least movies watched is {least_viewed_date[0]}.", None
        elif key == 'date_avg_rating':
            date_ratings = list(values)
            best_day = max(date_ratings, key=lambda x: x[1])
            worst_day = min(date_ratings, key=lambda x: x[1])
            yield f"The day with the best average rating is {best_day[0]}.", None
            yield f"The day with the worst average rating is {worst_day[0]}.", None
        elif key == 'genre_movie_rating':
            genre_movies = {}
            for (genre, movie), avg_rating in values:
                if genre not in genre_movies:
                    genre_movies[genre] = []
                genre_movies[genre].append((movie, avg_rating))
            
            for genre, movies in genre_movies.items():
                best_movie = max(movies, key=lambda x: x[1])
                worst_movie = min(movies, key=lambda x: x[1])
                yield f"The best movie in the {genre} genre is {best_movie[0]} with an average rating of {best_movie[1]:.2f}.", None
                yield f"The worst movie in the {genre} genre is {worst_movie[0]} with an average rating of {worst_movie[1]:.2f}.", None

if __name__ == '__main__':
    MovieAnalysis.run()