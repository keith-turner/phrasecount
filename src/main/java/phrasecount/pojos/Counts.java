package phrasecount.pojos;

import com.google.common.base.Objects;

public class Counts {
  // number of documents a phrase was seen in
  public final long documents;
  // total times a phrase was seen in all documents
  public final long total;

  public Counts() {
    documents = 0;
    total = 0;
  }

  public Counts(long docs, long total) {
    this.documents = docs;
    this.total = total;
  }

  public Counts add(Counts other) {
    return new Counts(this.documents + other.documents, this.total + other.total);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Counts) {
      Counts opc = (Counts) o;
      return opc.documents == documents && opc.total == total;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return (int) (993 * total + 17 * documents);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("documents", documents).add("total", total).toString();
  }
}
