#include <iostream>
using namespace std;

int main(){
    long n;
    cin >> n;

    for (int i = 2; i <= n; i++) {
        // 以上i只是范围，如果不是质数需要跳过
        bool isPrime = true;
        for (int j = 2; j <= i; j++){
            if(i == 2)
                break;
            if (i % j == 0 && i != j){
                isPrime = false;
                break;
            }
        }
        if (isPrime){
            if(n % i == 0){
                cout << i << " ";
            }else
                continue;
        }
    }
    cout << endl;


    return 0;
}