import React from "react";
import { Page, Text, View, Document, StyleSheet } from "@react-pdf/renderer";

// Create styles
const styles = StyleSheet.create({
  page: {
    flexDirection: "row",
    backgroundColor: "#E4E4E4",
  },
  section: {
    margin: 10,
    padding: 10,
    flexGrow: 1,
  },
});

// Create Document Component
export function MyDocument({ url, doc }: { url: string; doc: any }) {
  console.log(doc);

  return (
    <Document>
      <Page size="A4" style={styles.page}>
        <View style={styles.section}>
          <Text>
            <a href="https://www.peterbe.com">External links</a> example.
          </Text>
        </View>
        <View style={styles.section}>
          <Text>
            Section #4 Current URL: <code>{url}</code>
          </Text>
        </View>
      </Page>
    </Document>
  );
}
