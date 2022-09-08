package bitcoin_reader

import "testing"

func Test_sizeString(t *testing.T) {
	tests := []struct {
		size uint64
		want string
	}{
		{
			size: 123,
			want: "123 B",
		},
		{
			size: 1234,
			want: "1.23 KB",
		},
		{
			size: 123456,
			want: "123.46 KB",
		},
		{
			size: 1234567,
			want: "1.23 MB",
		},
		{
			size: 12345678,
			want: "12.35 MB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := sizeString(tt.size)

			if got != tt.want {
				t.Errorf("Wrong size string : got %s, want %s", got, tt.want)
			}
		})
	}
}
