package redisstore

const prefix = "ridematch"

func ActiveDriversKey() string { return prefix + ":drivers:active" }

func DriverKey(driverID string) string { return prefix + ":driver:" + driverID }

func CellKey(h3Cell string) string { return prefix + ":drivers:cell:" + h3Cell }
