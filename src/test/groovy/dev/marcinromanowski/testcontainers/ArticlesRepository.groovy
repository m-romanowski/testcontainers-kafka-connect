package dev.marcinromanowski.testcontainers

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import org.bson.Document

interface ArticlesRepository {
    ArticleEntity save(ArticleEntity entity)
    Optional<ArticleEntity> findByTitle(String title)
}

class ArticlesMongoRepository implements ArticlesRepository {

    private static final String ID_ROW = "id"
    private static final String TITLE_ROW = "title"
    private static final String DESCRIPTION_ROW = "description"

    private final MongoClient mongoClient
    private final String database
    private final String collection

    ArticlesMongoRepository(String uri, String database, String collection) {
        this.mongoClient = new MongoClient(new MongoClientURI(uri))
        this.database = database
        this.collection = collection
    }

    @Override
    ArticleEntity save(ArticleEntity entity) {
        MongoCollection<Document> articlesCollection = getArticlesCollection()
        Document articleDocument = new Document()
                .append(ID_ROW, entity.id)
                .append(TITLE_ROW, entity.title)
                .append(DESCRIPTION_ROW, entity.description)
        articlesCollection.insertOne(articleDocument)
        return entity
    }

    @Override
    Optional<ArticleEntity> findByTitle(String title) {
        MongoCollection<Document> articlesCollection = getArticlesCollection()
        return Optional.ofNullable(articlesCollection.find(Filters.eq("title", title)).first())
                .map(document -> new ArticleEntity(document.get(ID_ROW) as String, document.get(TITLE_ROW) as String, document.get(DESCRIPTION_ROW) as String))
    }

    private MongoDatabase getDatabase() {
        return mongoClient.getDatabase(database)
    }

    private MongoCollection<Document> getArticlesCollection() {
        return getDatabase().getCollection(collection)
    }

}

class ArticleEntity {

    String id
    String title
    String description

    ArticleEntity(String id, String title, String description) {
        this.id = id
        this.title = title
        this.description = description
    }

}
