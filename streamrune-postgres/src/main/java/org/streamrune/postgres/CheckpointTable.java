package org.streamrune.postgres;

/**
 * The checkpoint tables {@link PostgresCheckpointFence} can guard, and the only SQL identifiers it
 * ever splices into a statement: no other table name, and no caller text, reaches its SQL.
 */
enum CheckpointTable {

  /**
   * {@code subscription_checkpoint}, in the event-store series: the checkpoint of a subscription
   * that reads the global stream, kept with the events.
   */
  SUBSCRIPTION("subscription_checkpoint"),

  /**
   * {@code readmodel_checkpoint}, in the read-model series: the checkpoint of a projection, kept in
   * the database that holds its read models.
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
