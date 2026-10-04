package org.streamrune.aws;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;

/**
 * The auto-wired {@link ForgetSubjectService} must complete a GDPR forget on the AWS KMS backend —
 * the crypto-shred step records a tombstone (so decrypt yields {@code [REDACTED]}) and the
 * registered read-model purgers run: {@code deleteKey} records a tombstone instead of throwing.
 *
 * <p>Uses a mocked {@link KmsClient} as the KMS fake (no LocalStack required).
 */
class AwsKmsForgetSubjectServiceTest {

  @Test
  void forget_onKms_shredsSubjectAndRunsPurger() {
    KmsClient kms = mock(KmsClient.class);
    AwsKmsCryptoEngine engine =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kms)
            .kmsKeyId("test-key")
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();

    SubjectDataPurger purger = mock(SubjectDataPurger.class);
    when(purger.name()).thenReturn("orders-read-model");

    ForgetSubjectService service =
        ForgetSubjectService.builder().cryptoEngine(engine).purgers(List.of(purger)).build();

    SubjectId subject = SubjectId.of("customer-42");

    // The forget must NOT throw: deleteKey records a tombstone, so ForgetSubjectService completes.
    service.forget(subject, null);

    // Read-model purge ran (the only erasure available for a shared-CMK backend).
    verify(purger).purge(subject);

    // And the crypto-shred took effect: a subsequent decrypt yields [REDACTED] via
    // KeyNotFoundException, without ever hitting KMS.
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, "old-ct".getBytes()));
    verify(kms, never()).decrypt(any(DecryptRequest.class));
  }
}
