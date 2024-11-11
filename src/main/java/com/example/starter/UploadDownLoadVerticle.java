package com.example.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.CopyOptions;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class UploadDownLoadVerticle extends AbstractVerticle {

  public static void main(String[] args) {
    Vertx.vertx().deployVerticle(new UploadDownLoadVerticle());
  }

  Router router;
  PgPool client;

  PgConnectOptions connectOptions = new PgConnectOptions()
    .setPort(5432)
    .setHost("localhost")
    .setDatabase("postgres")
    .setUser("postgres")
    .setPassword("Admin_1234");

  PoolOptions poolOptions = new PoolOptions().setMaxSize(5);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    client = PgPool.pool(vertx, connectOptions, poolOptions);
    router = Router.router(vertx);
    router.route("/").handler(
      req -> {
        req.response()
          .putHeader("content-type", "text/plain")
          .end("Hello from Vert.x!");
      }
    );

    router.route("/download").handler(
      req -> {
        client.getConnection(ar1 -> {
          if (ar1.succeeded()) {
            System.out.println("Connected");
            SqlConnection conn = ar1.result();
            conn.preparedQuery("select name, enzyme_type, recognition_site, top_strand_cut_position, bottom_strand_cut_position from test")
              .execute(ar2 -> {
                conn.close();

                if (ar2.succeeded()) {
                  String data = prepareCsvContent(ar2);

                  Buffer buffer = Buffer.buffer();
                  buffer.appendBytes(data.getBytes(StandardCharsets.UTF_8));
                  req.response()
                    .putHeader("content-type", "text/csv")
                    .putHeader("Content-Disposition", "attachment;filename=Exported_data_from_test.csv")
                    .putHeader("Param", "no-cache")
                    .putHeader("Cache-Control", "no-cache")
                    .end(buffer);
                } else {
                  req.response()
                    .putHeader("content-type", "text/plain")
                    .end(ar2.cause().toString());
                }
              });
          } else {
            System.out.println("Could not connect:" + ar1.cause().getMessage());
          }
        });
      }
    );

    router.route().handler(BodyHandler.create().setUploadsDirectory("uploads").setBodyLimit(1000 * 1024));
    router.post("/upload").handler(routingContext -> {
      FileUpload fileUpload = routingContext.fileUploads().get(0);
      String uploadedFileName = fileUpload.uploadedFileName();
      String newFileName = "uploads/" + fileUpload.fileName();

      //Check file type
      String fileName = fileUpload.fileName();
      String fileExt = fileName.substring(fileName.lastIndexOf('.') + 1);
      if (!fileExt.equalsIgnoreCase("csv")) {
        routingContext.response().setStatusCode(400).end("Invalid file format");
        return;
      }

      /**
       * 1. Move uploaded to a local dir
       * 2. Check Csv content and save to db
       */
      CopyOptions copyOptions = new CopyOptions();
      copyOptions.setReplaceExisting(true);
      vertx.fileSystem().move(uploadedFileName, newFileName, copyOptions, result -> {
        if (result.succeeded()) {
          List<List<String>> checkedData = checkData(newFileName, 5);
          saveDataToDb(checkedData);
          routingContext.response().end("Successfully uploaded");
        } else {
          routingContext.response().setStatusCode(500).end("Failed to upload");
        }
      });
    });

    vertx.createHttpServer().requestHandler(router).listen(8899, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 8899");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }

  private void saveDataToDb(List<List<String>> checkedData) {
    client.getConnection(ar1 -> {
      if (ar1.succeeded()) {
        SqlConnection conn = ar1.result();

        String insertSql = prepareInsertSql(checkedData);

        conn.preparedQuery(insertSql)
          .execute(ar2 -> {
            conn.close();

            if (ar2.succeeded()) {
              System.out.println("Save success！");
            } else {
              throw new RuntimeException("Save error");
            }
          });
      } else {
        System.out.println("Could not connect:" + ar1.cause().getMessage());
      }
    });
  }

  private static String prepareInsertSql(List<List<String>> checkedData) {
    StringBuilder sb = new StringBuilder("INSERT INTO test (name, enzyme_type, recognition_site, top_strand_cut_position, bottom_strand_cut_position) VALUES ");
    for (int i = 1; i < checkedData.size(); i++) {
      List<String> row = checkedData.get(i);
      sb.append("(");

      StringBuilder sbValues = new StringBuilder();
      row.forEach(value -> {
        if (!value.matches("-?\\d+(\\.\\d+)?"))
          sbValues.append("'" + value + "'").append(",");
        else
          sbValues.append(value + ",");
      });
      String values = sbValues.toString().replaceAll(",$", "");
      sb.append(values);
      sb.append("),");
    }
    String insertSql = sb.toString().replaceAll(",$", ";");
    return insertSql;
  }

  private static String prepareCsvContent(AsyncResult<RowSet<Row>> ar2) {
    StringBuilder sb = new StringBuilder("name,enzyme_type,recognition_site,top_strand_cut_position,top_strand_cut_position\n");
    ar2.result().forEach(item -> {
      sb.append(item.getValue("name").toString() + ",")
        .append(item.getValue("enzyme_type").toString() + ",")
        .append(item.getValue("recognition_site").toString() + ",")
        .append(item.getValue("top_strand_cut_position").toString() + ",")
        .append(item.getValue("bottom_strand_cut_position").toString())
        .append("\n");
    });
    String data = sb.toString().replaceAll("\\n$", "");
    return data;
  }

  private static List<List<String>> checkData(String newFileName, int columnNumber) {
    List<List<String>> rows = new ArrayList<>();
    File file = new File(newFileName);
    try {
      Scanner scanner = new Scanner(file);
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        String[] values = line.split(",");

        if (values.length < columnNumber || Arrays.stream(values).anyMatch(v -> null == v || v.equals("null") || v.trim().equals("")))
          throw new RuntimeException("All fields can't be empty");

        List<String> row = new ArrayList<>();
        for (String value : values) {
          row.add(value);
        }
        rows.add(row);
      }
      scanner.close();
      return rows;
    } catch (FileNotFoundException e) {
      throw new RuntimeException("File not found");
    } catch (Exception e) {
      throw new RuntimeException("Failed to check file");
    }
  }

}
