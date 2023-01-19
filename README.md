# Discord Drive
This repo hold an MVP implementation of an concept to store data on discord by dividing them into chunks of 7MB each, which are then uploaded to discord using webhooks (because bots have higher ratelimits).
The attachment URLs of the chunks are then stored into firestore DB, which would be used to retrieve the chunks and merge them in order to get the file.