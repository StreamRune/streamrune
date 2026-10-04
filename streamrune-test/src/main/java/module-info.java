open module org.streamrune.test {
  requires org.streamrune.core;
  requires org.streamrune.crypto;
  requires org.junit.jupiter.api;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires com.fasterxml.jackson.datatype.jsr310;
  requires org.slf4j;

  exports org.streamrune.test;
}
