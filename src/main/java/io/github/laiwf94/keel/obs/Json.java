package io.github.laiwf94.keel.obs;

import java.util.List;
import java.util.Map;

/** Just enough JSON writing for logs and status pages. Keel never parses JSON. */
public final class Json {
  private Json() {}

  public static void string(StringBuilder sb, String s) {
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < 0x20 || c == 0x2028 || c == 0x2029) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    sb.append('"');
  }

  public static void value(StringBuilder sb, Object v) {
    switch (v) {
      case null -> sb.append("null");
      case Number n -> sb.append(n);
      case Boolean b -> sb.append(b);
      case Map<?, ?> m -> {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> e : m.entrySet()) {
          if (!first) {
            sb.append(',');
          }
          first = false;
          string(sb, String.valueOf(e.getKey()));
          sb.append(':');
          value(sb, e.getValue());
        }
        sb.append('}');
      }
      case List<?> l -> {
        sb.append('[');
        for (int i = 0; i < l.size(); i++) {
          if (i > 0) {
            sb.append(',');
          }
          value(sb, l.get(i));
        }
        sb.append(']');
      }
      default -> string(sb, v.toString());
    }
  }

  public static String toJson(Object v) {
    StringBuilder sb = new StringBuilder();
    value(sb, v);
    return sb.toString();
  }
}
