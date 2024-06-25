#include <iostream>
using namespace std;

int main(){
    long n;
    cin >> n;

    long circumference = 0;
    for (long i = 1; i <= n; i++){
        circumference = circumference-(i-1)+3*i+1;
    }
    cout << circumference << endl;

    long area = 0;
    for (long i = 1; i <= n; i++){
        area = area + i*i;
    }
    cout << area << endl;

    return 0;
}