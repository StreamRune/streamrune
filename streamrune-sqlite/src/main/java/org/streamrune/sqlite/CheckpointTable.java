package org.streamrune.sqlite;

/**
 * The checkpoint tables {@link SqliteCheckpointFence} can guard, and the only SQL identifiers it
 * ever splices into a statement: no other table name, and no caller text, reaches its SQL.
 */
enum CheckpointTable {

  /**
   * {@code subscription_checkpoint}, in the event-store series: the checkpoint of a subscription
   * that reads the global stream, kept with the events ({@link SqliteOffsetStore}).
   */
  SUBSCRIPTION("subscription_checkpoint"),

  /**
   * {@code readmodel_checkpoint}, in the read-model series: the checkpoint of a projection, kept in
   * the database that holds its read models ({@link SqliteReadModelCheckpoints}).
   */
  READ_MODEL("readmodel_checkpoint");

  private final String tableName;

  CheckpointTable(String tableName) {
    this.tableName = tableName;
  }

  /** The table's name, as it appears in the shipped migration. */
  String tableName() {
    return tableName;
  }
}
