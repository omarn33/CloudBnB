# MongoDB Queries

### Query 1
```
db.listings.aggregate([
	// Filter out city
	{
		$match: {
				city: "portland"
		}
	},

	// Join with the calendar collection
	{
		$lookup:{
		    from: "calendar",      
		    localField: "_id",   
		    foreignField: "_id", 
		    as: "calendar"
		}
	},
	{ $unwind: "$calendar" },
	{ $unwind: "$calendar.availability_periods" },
	
	// Filter out the 2-day period
	{
		$match: {
			$and: [
				{"calendar.availability_periods.start_date": new Date("2022-03-01T00:00:00.000Z") },
				{"calendar.availability_periods.end_date": new Date("2022-03-02T00:00:00.000Z") }
			]
		}
	},
	
	// Project
	{
		$project: {
			_id: 1,
			listing_name: 1,
			neighborhood: 1,
			room_type: 1,
			accommodates: 1,
			property_type: 1,
			amenities: 1,
			price: 1,
			avg_rating: 1,
			start_date: "$calendar.availability_periods.start_date",
			end_date: "$calendar.availability_periods.end_date"
		}
	},
	
	// Sort by descending order
	{ $sort : { avg_rating : -1 } }
]).pretty();
```

### Query 3
```
db.listings.aggregate([
    // Filter city and room type
    {
        $match: {
            $and: [
                {city: "los-angeles"},
                {room_type: "Entire home/apt"}
            ]
        }
    },

    // Join with the calendar collection
    {
        $lookup:{
            from: "calendar",      
            localField: "_id",   
            foreignField: "_id", 
            as: "calendar"
        }
    },
    { $unwind: "$calendar" },
    { $unwind: "$calendar.availability_periods" },

    // Find available periods where the total nights is greater than the minimun nigths requirement
    {
        $match: {
            $and: [
                {"calendar.availability_periods.available": true},
                {"calendar.availability_periods.start_date": { $gte: new Date("2022-03-01T00:00:00.000Z") }},
                {"calendar.availability_periods.end_date": { $lte: new Date("2022-03-31T00:00:00.000Z") }},
                {$expr: {$gte: ["$calendar.availability_periods.total_nights", "$calendar.min_nights"]}}
            ]
        }
    },

    // Project
    {
        $project: {
            _id: 1,
            listing_name: 1,
            neighborhood: 1,
            start_date: "$calendar.availability_periods.start_date",
            end_date: "$calendar.availability_periods.end_date",
            total_nights: "$calendar.availability_periods.total_nights",
            min_nights: "$calendar.min_nights"
        }
    }

]).pretty();
```

### Query 4
```
db.listings.aggregate([
    // Filter city and room type
    {
        $match: {
            $and: [
                {city: "portland"},
                {room_type: "Entire home/apt"}
            ]
        }
    },
    
    // Join with calendar collection
    {
        $lookup:{
            from: "calendar",      
            localField: "_id",   
            foreignField: "_id", 
            as: "calendar"
        }
    },
    { $unwind: "$calendar" },
    { $unwind: "$calendar.availability_periods" },
    
    // Find available periods for a given time interval
    {    
        $match: {
			$and: [
				{"calendar.availability_periods.available": true},
				{"calendar.availability_periods.start_date": { $gte: new Date("2022-03-01T00:00:00.000Z") }},
				{"calendar.availability_periods.end_date": { $lte: new Date("2022-08-31T00:00:00.000Z") }}
			]
        }
    },
    
    // Sum all available nights
    {
        $group: {
            _id: "$_id",
            "total_available_nights": {$sum: "$calendar.availability_periods.total_nights"}
        }
    }
	
]).pretty();
```
