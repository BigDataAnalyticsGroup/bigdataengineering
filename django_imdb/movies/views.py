from django.db.models import Count
from django.shortcuts import render
from django.views.generic import ListView
from movies.models import Movie, Actor

from .forms import MovieForm
from .library import show_queries, clean_queries


def render_with_queries(request, template, context):
    '''
    Render the given template with the given context and show the queries
    :param request: request object
    :param template: template name
    :param context: context dictionary
    :return: rendered template
    '''
    r = render(request, template, context)
    r.write("<h4>SQL-queries issued:</h4>")
    r.write("<pre>")
    r.write(show_queries())
    r.write("</pre>")

    return r


def index(request):
    '''
    Display list of movies
    '''
    movies = Movie.objects.order_by("-rank").prefetch_related("director_set__movies").all()

    context = {
        "movies": movies,
    }
    return render_with_queries(request, "movies/index.html", context)


class MovieListView(ListView):
    # Use Generic View to display list of movies
    model = Movie
    template_name = "movies/movie_list.html"  # use default template name, can be overriden here


def get_movie_search(request):
    '''
    Display movie search form
    '''
    query = request.GET.get('query')
    movie = None
    actors = None
    if query is not None:
        try:
            movie = Movie.objects.get(name=query)
            actors = Actor.objects.filter(movies=movie.pk)
        except:
            movie = []
            actors = []
    context = {
        "movie": movie,
        "actors": actors,
        "form": MovieForm,
    }
    return render_with_queries(request, "movies/movie_search.html", context)


def get_movie_details(request, pk):
    '''
    Display movie details
    :param pk: pk of the movie
    '''
    movie = None
    actors = None
    if pk is not None:
        try:
            movie = Movie.objects.get(pk=pk)
            actors = Actor.objects.filter(movies=movie.pk)
        except:
            movie = []
            actors = []
    context = {
        "movie": movie,
        "actors": actors,
    }
    return render_with_queries(request, "movies/movie_details.html", context)


def movie_statistics(request):
    # Display number of movies per year filtered by year >= 1999
    movies_group_by_year_count = Movie.objects.filter(
        year__gte=1999
    ).values('year').annotate(
        total=Count('year')
    ).order_by('year')

    context = {
        "movies_group_by_year_count": movies_group_by_year_count,
    }
    return render_with_queries(request, "movies/movie_statistics.html", context)


def movie_statistics2(request):
    # Display number of movies per year
    movies_group_by_year_count = Movie.objects.filter(genre__name="Action").values('year').annotate(
        total=Count('year')).order_by('year')

    context = {
        "movies_group_by_year_count": movies_group_by_year_count,
    }
    return render_with_queries(request, "movies/movie_statistics.html", context)
