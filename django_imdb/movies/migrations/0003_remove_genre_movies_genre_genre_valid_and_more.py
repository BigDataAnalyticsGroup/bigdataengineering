# Generated by Django 4.2.1 on 2023-06-06 10:54

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("movies", "0002_rename_genre_genre_name"),
    ]

    operations = [
        migrations.RemoveConstraint(
            model_name="genre",
            name="movies_genre_genre_valid",
        ),
        migrations.AddConstraint(
            model_name="genre",
            constraint=models.CheckConstraint(
                check=models.Q(
                    (
                        "name__in",
                        [
                            "Action",
                            "Adventure",
                            "Animation",
                            "Biography",
                            "Comedy",
                            "Crime",
                            "Documentary",
                            "Drama",
                            "Family",
                            "Fantasy",
                            "Film-Noir",
                            "History",
                            "Horror",
                            "Music",
                            "Musical",
                            "Mystery",
                            "Romance",
                            "Sci-Fi",
                            "Short",
                            "Sport",
                            "Thriller",
                            "War",
                            "Western",
                        ],
                    )
                ),
                name="movies_genre_name_valid",
            ),
        ),
    ]