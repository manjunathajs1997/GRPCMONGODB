/*
 * Copyright 2015 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.examples.helloworld;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Sorts.descending;
import org.bson.Document;
import com.mongodb.client.*;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class HelloWorldServer {
  private static final Logger logger = Logger.getLogger(HelloWorldServer.class.getName());

  private Server server;

  private void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
        .addService(new GreeterImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        try {
          HelloWorldServer.this.stop();
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
        }
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    final HelloWorldServer server = new HelloWorldServer();
    server.start();
    server.blockUntilShutdown();
  }

  static class GreeterImpl extends GreeterGrpc.GreeterImplBase {

    @Override
    public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
      CalculateCost cst=new CalculateCost();
      String abc=task1(cst);
      HelloReply reply = HelloReply.newBuilder().setMessage("Hello manju" + req.getType()).build();

      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }

    public String task1( CalculateCost calculateCost){
      String connectionString = System.getProperty("mongodb://localhost:27017/");
      Integer totalcost=0;
      try(MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017/"))
      {
        MongoDatabase database = mongoClient.getDatabase("GRPC");
        try{
          MongoCollection<Document> eduCostStatCollection = database.getCollection("EduCostStat");
          logger.info("in meth");
//          FindIterable<Document> educostList=  eduCostStatCollection.find(new Document("Year", calculateCost.getYear()).append("State",calculateCost.getState())
//                  .append("Type",calculateCost.getType()).append("Length",calculateCost.getLength())
//                  .append("Expense",calculateCost.getExpense()));
          FindIterable<Document> educostList=  eduCostStatCollection.find(new Document("Year", "2013").append("State","Alabama")
                  .append("Type","Private").append("Length","4-Year")
                  .append("Expense","Room/Board"));
          String completeQuery ="eduCostStatCollection.find(new Document('Year',"+ calculateCost.getYear()+").append('State',"+calculateCost.getState()+").append('Type',"+calculateCost.getType()+").append('Length',"+calculateCost.getLength()+").append('Expense',"+calculateCost.getExpense()+"))";
          MongoCollection<Document> eduCostStatoneCollection = database.getCollection("EduCostStatQueryOne");
          eduCostStatoneCollection.insertOne(new Document("query",completeQuery));
          MongoCursor<Document> it = educostList.iterator();

          while (it.hasNext()) {
            int val=Integer.parseInt(it.next().get("Value").toString());
            totalcost=totalcost+val;
          }
          System.out.println(totalcost);
        }catch (Exception ex){

        }
      }
      String s=String.valueOf(totalcost);
      return String.valueOf(totalcost);
    }

  }
}
