package org.streamrune.core.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;

class SpanAttributesTest {

  @Test
  void keepsTheOrderOfTheKeysAndTheirTypedValues() {
    SpanAttributes attributes =
        SpanAttributes.builder()
            .put("streamrune.command.type", "PlaceOrder")
            .put("streamrune.batch.size", 3L)
            .put("streamrune.snapshot.used", true)
            .build();

    List<String> keys = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    attributes.forEach(
        (key, value) -> {
          keys.add(key);
          values.add(value);
        });

    assertThat(keys)
        .containsExactly(
            "streamrune.command.type", "streamrune.batch.size", "streamrune.snapshot.used");
    assertThat(values).containsExactly("PlaceOrder", 3L, true);
    assertThat(attributes.size()).isEqualTo(3);
    assertThat(attributes.isEmpty()).isFalse();
    assertThat(attributes.asMap()).containsKeys("streamrune.command.type");
  }

  @Test
  void aNullStringValue_isNoAttribute() {
    SpanAttributes attributes =
        SpanAttributes.builder()
            .put("streamrune.aggregate.id", (String) null)
            .put("streamrune.command.type", "PlaceOrder")
            .build();

    assertThat(attributes.asMap()).containsOnlyKeys("streamrune.command.type");
  }

  @Test
  void aKeyPutTwice_keepsTheLastValueInTheFirstPosition() {
    SpanAttributes attributes =
        SpanAttributes.builder()
            .put("streamrune.a", "1")
            .put("streamrune.b", "2")
            .put("streamrune.a", "3")
            .build();

    assertThat(attributes.asMap().keySet()).containsExactly("streamrune.a", "streamrune.b");
    assertThat(attributes.asMap()).containsEntry("streamrune.a", "3");
  }

  @Test
  void aBlankOrNullKey_isRefused() {
    var builder = SpanAttributes.builder();
    assertThatThrownBy(() -> builder.put(" ", "v")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.put(null, 1L)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.put("", true)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void anEmptyBuild_isTheSharedEmptyInstance() {
    assertThat(SpanAttributes.builder().build()).isSameAs(SpanAttributes.empty());
    assertThat(SpanAttributes.builder().put("streamrune.a", (String) null).build())
        .isSameAs(SpanAttributes.empty());
    assertThat(SpanAttributes.empty().isEmpty()).isTrue();
    assertThat(SpanAttributes.empty().size()).isZero();
  }

  @Test
  void theMapView_isUnmodifiable() {
    SpanAttributes attributes = SpanAttributes.builder().put("streamrune.a", "1").build();

    assertThatThrownBy(() -> attributes.asMap().put("streamrune.b", "2"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void equalAttributes_areEqual() {
    SpanAttributes one = SpanAttributes.builder().put("streamrune.a", "1").put("k", 2L).build();
    SpanAttributes two = SpanAttributes.builder().put("streamrune.a", "1").put("k", 2L).build();
    SpanAttributes other = SpanAttributes.builder().put("streamrune.a", "1").put("k", 3L).build();
    SpanAttributes otherKey =
        SpanAttributes.builder().put("streamrune.b", "1").put("k", 2L).build();

    assertThat(one).isEqualTo(two).hasSameHashCodeAs(two).isNotEqualTo(other);
    assertThat(one).isNotEqualTo(otherKey).isNotEqualTo("streamrune.a");
    assertThat(one.toString()).contains("streamrune.a=1", "k=2");
  }

  @Test
  void aSpanLink_requiresBothIds() {
    assertThatThrownBy(() -> new SpanLink(null, SpanId.of("s")))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new SpanLink(TraceId.of("t"), null))
        .isInstanceOf(NullPointerException.class);
  }
}
