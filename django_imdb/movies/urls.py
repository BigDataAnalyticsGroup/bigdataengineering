from django.urls import path
from . import views
from movies.views import MovieListView, movie_statistics, get_movie_search, get_movie_details, movie_statistics2

urlpatterns = [
    path("", views.index, name="movies_index"),
    path("details/<str:pk>", get_movie_details, name="movie_details"),
    path("search/", get_movie_search, name="movie_search"),
    path("movies/", MovieListView.as_view(), name="movie_list_view"),
    path("movie_statistics/", movie_statistics, name="movie_statistics"),
    path("movie_statistics2/", movie_statistics2, name="movie_statistics2"),
]
