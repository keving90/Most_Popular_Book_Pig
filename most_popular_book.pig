-- This Pig script uses the Book-Crossing dataset (http://www2.informatik.uni-freiburg.de/~cziegler/BX/)
-- to find the most popular book. The book with the highest number of explicit ratings
-- is considered to be the most popular book.

-- Import the PiggyBank library for CSVExcelStorage()
-- CSVExcelStorage() is useful because it emliminates the header row
REGISTER 'lib/piggybank.jar';

-- Predefine CSVExcelStorage() for easy use
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

-- Give the job name a title
SET job.name 'Most Popular Book';

-- Set out the schema containing book ratings
book_ratings = LOAD 'book_ratings/BX-Book-Ratings.csv' USING CSVExcelStorage() AS
(
	UserID: int,
	ISBN: chararray,
	Rating: int
);

-- Set up the schema containing books
books = LOAD 'book_ratings/BX-Books.csv' USING CSVExcelStorage() AS
(
	ISBN: chararray,
	BookTitle: chararray,
	BookAuthor: chararray,
	PublicationYear: int,
	Publisher: chararray,
	ImageURL_S: chararray,
	ImageURL_M: chararray,
	ImageURL_L: chararray
);

-- Remove implicit ratings (0 rating scores) from the book_ratings bag
explicit_ratings = FILTER book_ratings BY (Rating > 0);

-- Group the explicit book ratings by ISBN
grouped_ratings = GROUP explicit_ratings BY ISBN;

-- Get the number of explicit ratings for each book
rating_count = FOREACH grouped_ratings GENERATE group AS ISBN, COUNT(explicit_ratings.UserID) AS NumRatings;

-- Only interested in ISBN, title, and author in books bag
book_info = FOREACH books GENERATE ISBN, BookTitle, BookAuthor;

-- Perform an inner join on the average_rating and book_info bags
inner_join = JOIN rating_count BY ISBN, book_info BY ISBN;

-- The inner_join bag has two ISBN fields, which is redundant
results = FOREACH inner_join GENERATE book_info::BookTitle AS BookTitle, book_info::BookAuthor AS BookAuthor, rating_count::NumRatings AS NumRatings;

-- Sort the data
sorted_results = ORDER results BY NumRatings DESC;

-- Execute all of the above actions and store the results
STORE sorted_results INTO 'most_popular_book';












































