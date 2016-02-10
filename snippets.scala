// Open file
val file = sc.textFile("data/genres.list")

// To map
val fileData = file.map(x => x.split(";"))

// View some data
fileData.first

// Map all years
val fileYears = fileData.map(x => (x(1),1))

// Reduce by year
val fileYearsCount = fileYears.reduceByKey(_+_)

// Filter stuff
val fileYearsFiltered = fileYearsCount.filter(x => x._2 > 1000)

// Sort and print
fileYearsFiltered.sortBy(x => x, true, 0).foreach(println)