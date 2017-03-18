CREATE TABLE IF NOT EXISTS "markets" (
	"regionID" int8 NOT NULL,
	"typeID" int8 NOT NULL,
	"market" bytea NOT NULL,
	PRIMARY KEY ("regionID", "typeID")
);

CREATE INDEX "regionID" ON "markets" ("regionID");
CREATE INDEX "typeID" ON "markets" ("typeID");