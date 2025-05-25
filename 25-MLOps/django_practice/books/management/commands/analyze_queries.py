from django.core.management.base import BaseCommand
from django.db import connection
from books.models import Author, Book

class Command(BaseCommand):
    help = 'Analyze Django ORM queries'

    def add_arguments(self, parser):
        parser.add_argument(
            '--demo',
            action='store_true',
            help='Run demo queries with analysis',
        )
        parser.add_argument(
            '--table',
            type=str,
            help='Specify table/model to analyze',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=10,
            help='Limit number of results',
        )

    def handle(self, *args, **options):
        if options['demo']:
            self.run_demo()
        else:
            self.stdout.write("Use --demo to run example queries")

    def analyze_query(self, queryset, description=""):
        """Analyze a Django queryset"""
        self.stdout.write(f"\n{'='*50}")
        self.stdout.write(f"ANALYZING: {description}")
        self.stdout.write(f"{'='*50}")
        
        # Get the compiled query with parameters
        with connection.cursor() as cursor:
            # Django's way to get the actual SQL with parameters
            compiled_sql, params = queryset.query.get_compiler(connection=connection).as_sql()
            
            # For displaying the raw SQL, we can do a simple string replacement
            # Note: This is for display only and not for executing EXPLAIN.
            sql_for_display = compiled_sql
            if params:
                # Replace Django's %s placeholders with their values for display
                # This is a simplification; for complex cases, you might need more robust handling.
                for param in params:
                    sql_for_display = sql_for_display.replace('%s', repr(param), 1)

            self.stdout.write(f"\nRAW SQL:\n{sql_for_display}\n")
            
            # Run EXPLAIN QUERY PLAN (SQLite equivalent)
            try:
                # IMPORTANT CHANGE HERE: Pass params separately to cursor.execute
                cursor.execute(f"EXPLAIN QUERY PLAN {compiled_sql}", params)
                results = cursor.fetchall()
                
                self.stdout.write("QUERY PLAN:")
                for row in results:
                    # SQLite EXPLAIN QUERY PLAN returns: id, parent, notused, detail
                    self.stdout.write(f"  {row[3]}")
            except Exception as e:
                self.stdout.write(f"Error analyzing query plan: {e}")
                # Fallback: just show the basic query structure
                # IMPORTANT CHANGE HERE: Pass params separately to cursor.execute
                try:
                    cursor.execute(f"EXPLAIN {compiled_sql}", params)
                    results = cursor.fetchall()
                    self.stdout.write("BASIC EXPLAIN:")
                    for row in results:
                        self.stdout.write(f"  {row}")
                except Exception as e_fallback:
                    self.stdout.write(f"Error with basic EXPLAIN: {e_fallback}")
        
        # Execute and show results count
        count = queryset.count()
        self.stdout.write(f"\nRESULT COUNT: {count}")

    def run_demo(self):
        self.stdout.write("Running Django Query Analysis Demo\n")
        
        # Query 1: Simple filter
        q1 = Author.objects.filter(name__icontains="george")
        self.analyze_query(q1, "Filter authors by name containing 'george'")
        
        # Query 2: Foreign key lookup (N+1 problem demo)
        q2 = Book.objects.all()
        self.analyze_query(q2, "Get all books (without select_related)")
        
        # Query 3: Optimized with select_related
        q3 = Book.objects.select_related('author')
        self.analyze_query(q3, "Get all books with select_related('author')")
        
        # Query 4: Filter with join
        q4 = Book.objects.filter(author__name="George Orwell")
        self.analyze_query(q4, "Books by George Orwell")
        
        # Query 5: Complex query
        q5 = Book.objects.filter(
            pages__gt=200,
            publication_date__year__lt=1950
        ).select_related('author')
        self.analyze_query(q5, "Books >200 pages published before 1950")

# Alternative: Direct shell analysis functions (also needs modification)
# Put this in a separate file or add to your shell sessions

def explain_queryset(queryset, description="Query"):
    """Quick function to analyze any queryset in Django shell"""
    from django.db import connection
    
    print(f"\n{'='*40}")
    print(f"ANALYZING: {description}")
    print(f"{'='*40}")
    
    # Get compiled SQL and parameters
    compiled_sql, params = queryset.query.get_compiler(connection=connection).as_sql()
    
    # For displaying the raw SQL, we can do a simple string replacement
    sql_for_display = compiled_sql
    if params:
        for param in params:
            sql_for_display = sql_for_display.replace('%s', repr(param), 1)
    
    print(f"\nSQL: {sql_for_display}\n")
    
    with connection.cursor() as cursor:
        try:
            # IMPORTANT CHANGE HERE: Pass params separately to cursor.execute
            cursor.execute(f"EXPLAIN QUERY PLAN {compiled_sql}", params)
            results = cursor.fetchall()
            
            print("EXECUTION PLAN:")
            for row in results:
                print(f"  {row[3]}")
        except Exception as e:
            print(f"Error with EXPLAIN QUERY PLAN: {e}")
            try:
                cursor.execute(f"EXPLAIN {compiled_sql}", params)
                results = cursor.fetchall()
                print("BASIC EXPLAIN:")
                for row in results:
                    print(f"  {row}")
            except Exception as e_fallback:
                print(f"Error with basic EXPLAIN: {e_fallback}")
    
    print(f"\nResult count: {queryset.count()}")
