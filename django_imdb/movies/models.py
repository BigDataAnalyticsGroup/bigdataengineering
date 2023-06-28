from django.db import models


class GENRE_CHOICES(models.TextChoices):
    ACTION = 'Action', 'Action'
    ADVENTURE = 'Adventure', 'Adventure'
    ANIMATION = 'Animation', 'Animation'
    BIOGRAPHY = 'Biography', 'Biography'
    COMEDY = 'Comedy', 'Comedy'
    CRIME = 'Crime', 'Crime'
    DOCUMENTARY = 'Documentary', 'Documentary'
    DRAMA = 'Drama', 'Drama'
    FAMILY = 'Family', 'Family'
    FANTASY = 'Fantasy', 'Fantasy'
    FILM_NOIR = 'Film-Noir', 'Film-Noir'
    HISTORY = 'History', 'History'
    HORROR = 'Horror', 'Horror'
    MUSIC = 'Music', 'Music'
    MUSICAL = 'Musical', 'Musical'
    MYSTERY = 'Mystery', 'Mystery'
    ROMANCE = 'Romance', 'Romance'
    SCIFI = 'Sci-Fi', 'Sci-Fi'
    SHORT = 'Short', 'Short'
    SPORT = 'Sport', 'Sport'
    THRILLER = 'Thriller', 'Thriller'
    WAR = 'War', 'War'
    WESTERN = 'Western', 'Western'


class Genre(models.Model):
    name = models.CharField(max_length=11, choices=GENRE_CHOICES.choices)

    def __str__(self):
        return f"{self.name}"

    class Meta:
        constraints = [
            models.CheckConstraint(
                name="%(app_label)s_%(class)s_name_valid",
                check=models.Q(name__in=GENRE_CHOICES.values),
            )
        ]
        db_table = "genre"


class Movie(models.Model):
    name = models.CharField(max_length=200)
    year = models.IntegerField()
    rank = models.FloatField()
    genre = models.ManyToManyField(Genre)

    class Meta:
        db_table = "movies"
        managed = True

    def __str__(self):
        return f"{self.name}, {self.year}, {self.rank}"


class GENDER_CHOICES(models.TextChoices):
    MALE = 'M', 'Male'
    FEMALE = 'F', 'Female'


class Actor(models.Model):
    first_name = models.CharField(max_length=30)
    last_name = models.CharField(max_length=30)
    gender = models.CharField(max_length=1, choices=GENDER_CHOICES.choices)
    movies = models.ManyToManyField(Movie, through='PlayIn')

    def __str__(self):
        return f"{self.first_name} {self.last_name}, {self.gender}"

    class Meta:
        managed = True
        db_table = "actors"
        constraints = [
            models.CheckConstraint(
                name="%(app_label)s_%(class)s_gender_valid",
                check=models.Q(gender__in=GENDER_CHOICES.values),
            )
        ]


class Director(models.Model):
    first_name = models.CharField(max_length=30)
    last_name = models.CharField(max_length=30)
    movies = models.ManyToManyField(Movie)

    def __str__(self):
        return f"{self.first_name} {self.last_name}"

    class Meta:
        db_table = "directors"


# Create custom through model to add 'role' field to many-to-many relationship
class PlayIn(models.Model):
    actor = models.ForeignKey(Actor, on_delete=models.CASCADE)
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE)
    role = models.TextField()

    class Meta:
        # Django does not support composite keys: https://code.djangoproject.com/wiki/MultipleColumnPrimaryKeys
        # avoid that the same person plays the same role multiple times in the same movie
        unique_together = (('actor', 'movie', 'role'),)
        db_table = "play_in"
