open module org.streamrune.aws.kms.crypto {
  requires transitive org.streamrune.core;
  requires transitive software.amazon.awssdk.services.kms;
  // The ForgottenSubjectStore SPI (and its in-memory default) live in the shared crypto-api
  // module so both the KMS and Vault backends consume one tombstone abstraction. Transitive so a
  // consumer of the public Builder.forgottenSubjectStore(ForgottenSubjectStore) sees the type.
  requires transitive org.streamrune.crypto;

  exports org.streamrune.aws;
}
