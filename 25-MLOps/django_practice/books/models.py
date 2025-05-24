from django.db import models
from django.utils import timezone

class Author(models.Model):
    name = models.CharField(max_length=100)
    birth_date = models.DateField(null=True, blank=True)
    biography = models.TextField(blank=True)

    def __str__(self):
        return self.name

class Book(models.Model):
    title = models.CharField(max_length=200)
    author = models.ForeignKey(Author, on_delete=models.CASCADE, related_name='books')
    publication_date = models.DateField(null=True, blank=True)
    genre = models.CharField(max_length=50, blank=True)
    pages = models.IntegerField(null=True, blank=True)
    # Automatically set when the book is created
    created_at = models.DateTimeField(auto_now_add=True)
    # Automatically updated every time the book is saved
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.title} by {self.author.name}"
