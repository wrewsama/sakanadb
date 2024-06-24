package repository	

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)
func Test_Repo(t *testing.T) {
	Convey("Test repo", t, func() {
		repo := NewRepo()
		repo.Set("k1", "v1")
		repo.Set("k2", "v2")

		val, ok := repo.Get("k1")
		So(val, ShouldEqual, "v1")
		So(ok, ShouldBeTrue)

		val, ok = repo.Get("k2")
		So(val, ShouldEqual, "v2")
		So(ok, ShouldBeTrue)

		repo.Delete("k2")
		val, ok = repo.Get("k2")
		So(val, ShouldEqual, nil)
		So(ok, ShouldBeFalse)
	})	
}