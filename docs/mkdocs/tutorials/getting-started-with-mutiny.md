# Getting started with Mutiny

## Using Mutiny in a Java application

Add the _dependency_ to your project using your preferred build tool:

=== "Apache Maven"

    ```xml
    <dependency>
        <groupId>io.smallrye.reactive</groupId>
        <artifactId>mutiny</artifactId>
        <version>{{ attributes.versions.mutiny }}</version>
    </dependency>
    ```

=== "Gradle (Groovy)"

    ```groovy
    implementation 'io.smallrye.reactive:mutiny:{{ attributes.versions.mutiny }}'
    ```

=== "Gradle (Kotlin)"

    ```kotlin
    implementation("io.smallrye.reactive:mutiny:{{ attributes.versions.mutiny }}")
    ```

=== "JBang"

    ```java
    //DEPS io.smallrye.reactive:mutiny:{{ attributes.versions.mutiny }}
    ```

## Using Mutiny with Quarkus

Most of the [Quarkus](https://quarkus.io) extensions with reactive capabilities already depend on Mutiny.

You can also add the `quarkus-mutiny` dependency explicitly from the command-line:

```bash
mvn quarkus:add-extension -Dextensions=mutiny
```

or by editing the `pom.xml` file and adding:

```xml
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-mutiny</artifactId>
</dependency>
```

## Hello Mutiny!

Once you made Mutiny available to your classpath, you can start writing code.
Let's start with this simple program:


```java linenums="1"
--8<-- "{{ attributes.snippet_dir }}/java/FirstProgram.java"
```

This program prints:

```
>> HELLO MUTINY
```

## Dissecting the pipeline

What's interesting is how this message is _built_.
We described a processing pipeline taking an item, processing it and finally consuming it.

First, we create a `Uni`, one of the two types with `Multi` that Mutiny provides.
A `Uni` is a stream emitting either a single item or a failure.

Here, we create a `Uni` emitting the `"hello"` item.
This is the input of our pipeline.
Then we process this item:

- we append `" mutiny"`, then
- we make it an uppercase string.

This forms the processing part of our pipeline, and then we finally **subscribe** to the pipeline.

This last part is essential.
If you don't have a final subscriber, nothing is going to happen.
Mutiny types are lazy, meaning that you need to express your interest.
If you don't the computation won't even start.

!!! important
    
    If your program doesn't do anything, verify that you didn't forget to subscribe!

## Mutiny uses a builder API!

Another important aspect is the pipeline construction.
Appending a new _stage_ to a pipeline returns a new `Uni.`

The previous program is equivalent to:

```java linenums="1"
{{ insert('java/FirstProgramTest.java', 'uni') }}
```

It is fundamental to understand that this program is not equivalent to:

```java linenums="1"
{{ insert('java/FirstProgramTest.java', 'uni2') }}
```

This program just prints `">> hello"`, as it does not use the appended stages and the final subscriber consumes the first `Uni.`

!!! warning

    Mutiny APIs are not fluent and each computation stage returns a new object.