package phrasecount;

public class PhraseCounts {
  //number of documents a phrase was seen in
  public long documents;
  //total times a phrase was seen in all documents
  public long total;

  public PhraseCounts(long docs, long total) {
    this.documents = docs;
    this.total = total;
  }

  public void add(PhraseCounts other) {
    this.documents += other.documents;
    this.total += other.total;
  }
}
